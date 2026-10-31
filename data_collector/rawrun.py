"""Measured raw owner shared by public-feed acquisition paths."""
import asyncio
from dataclasses import asdict, replace
import shutil
import time
import uuid

from .clocklog import DEFAULT_POLICY
from .clocksource import ChronyClockSampler
from .contracts import Provenance, RawRecord, Receipt, canonical, digest, strict_json
from .journal import Journal, JournalLimits, _publish_bytes
from .lane import ArchiveLane, DurableAck, PENDING_CLOCK_EPOCH
from .queues import ByteQueue
from .segments import _mkdir, iter_segment_records
from .writer import SessionWriter


class RawOwner:
    def __init__(self, root, config, observer, *, universe=None, batch_size=1,
                 clock_read=None, clock_wait=None):
        self.root,self.config,self.observer=root,config,observer
        self.universe,self.batch_size=universe,batch_size
        self.clock_read,self.clock_wait=clock_read,clock_wait
        self.writer=self.emergency=self.lane=self.sampler=None
        self.raw_task=self.clock_task=None
        self.positions={};self.closed=False;self.segments=[]

    async def start(self):
        from .app import _freeze_source
        _mkdir(self.root)
        disk=shutil.disk_usage(self.root)
        if disk.free<5*1024**3 or disk.free/disk.total<.10:
            raise ValueError('disk free below v2 stop threshold')
        source=_freeze_source(self.root)
        _publish_bytes(self.root/'config.json',self.config.to_bytes())
        if self.universe is None:
            pending=canonical(dict(schema='dc.pending-universe/1',scope='discovery not yet acquired'))
            _publish_bytes(self.root/'pending-universe.json',pending)
            universe_hash=digest(pending)
        else:universe_hash=digest(self.universe)
        self.provenance=Provenance(source,self.config.config_sha256,universe_hash)
        self.capture='capture-'+self.root.name
        self.process='process-'+uuid.uuid4().hex
        try:
            self.writer=SessionWriter(self.root/'wal',self.root/'archive',venue=self.config.venue,
                environment=self.config.environment,capture_id=self.capture,process_session_id=self.process,
                provenance=self.provenance,started_utc_ns=self.observer()[0],
                limits=JournalLimits(target_bytes=self.config.raw_segment_bytes))
            self.emergency=Journal(self.root/'emergency.wal',venue=self.config.venue,
                environment=self.config.environment,create=True)
            self.lane=ArchiveLane(self.writer,ByteQueue(),emergency_writer=self.emergency,
                observe=self.observer,clock_policy=DEFAULT_POLICY,clock_max_assessments=None,
                raw_batch_size=self.batch_size)
            self.raw_task=asyncio.create_task(self.lane.run())
            self.sampler=ChronyClockSampler(self.lane,executable=self.config.chronyc,
                max_samples=None,read=self.clock_read,wait_until=self.clock_wait)
            await self.sampler.prime()
            self.clock_task=asyncio.create_task(self.sampler.run())
        except BaseException:
            await self.close()
            raise
        return self

    def check(self):
        if self.lane.health()['writer_error'] is not None:
            raise ValueError('archive writer failed')
        if self.clock_task is not None and self.clock_task.done():
            self.clock_task.result()
            raise ValueError('clock sampler ended')

    async def aligned(self, stopped=lambda:False):
        deadline=time.monotonic()+self.config.warmup_timeout_seconds
        while not stopped() and not self.sampler.last_ack.assessment.aligned:
            self.check()
            if time.monotonic()>=deadline:raise TimeoutError('clock warmup timeout')
            self.sampler.updated.clear()
            try:await asyncio.wait_for(self.sampler.updated.wait(),.5)
            except TimeoutError:pass

    def admit(self, body, channel, kind, *, connection='public-http', product='multiplex', fields=None):
        self.check()
        utc,mono,_=self.observer();seq=self.positions.get(connection,0)
        record=RawRecord(Receipt(self.capture,self.process,connection,seq,utc,mono,PENDING_CLOCK_EPOCH),
            self.config.venue,self.config.environment,product,channel,kind,body,self.provenance,
            native_fields_json=canonical({} if fields is None else fields))
        future=self.lane.submit_received(record)
        self.positions[connection]=seq+1
        return record,future

    async def save(self, body, channel, kind, **kwargs):
        record,future=self.admit(body,channel,kind,**kwargs)
        ack=await future
        return self.confirm(record,ack)

    def confirm(self,record,ack):
        if not isinstance(ack,DurableAck) or not ack.timing_valid:
            raise ValueError('raw durability unconfirmed or late')
        fields=dict(strict_json(record.native_fields_json),collector_ingress_ordinal=ack.ingress_ordinal)
        stored=replace(record,receipt=replace(record.receipt,clock_epoch=ack.clock_epoch),native_fields_json=canonical(fields))
        if digest(stored.to_bytes())!=ack.raw_sha256:raise ValueError('durable raw hash differs')
        return stored

    async def close(self):
        from .app import _drain
        if self.closed:return
        self.closed=True
        error=None
        try:
            if self.sampler is not None:self.sampler.stop()
            if self.clock_task is not None:
                results=await _drain(asyncio.gather(self.clock_task,return_exceptions=True))
                if isinstance(results[0],Exception):error=results[0]
            if self.lane is not None:
                self.lane.stop()
                if self.raw_task is not None:await _drain(self.raw_task)
                self.segments=list(self.lane.segments)
                if self.lane.health()['writer_error'] is not None:error=error or ValueError('archive writer failed')
        finally:
            if self.emergency is not None:self.emergency.close()
            if self.writer is not None:self.writer.abort()
        if error is not None:raise error

    def descriptor(self):
        return dict(provenance=asdict(self.provenance),
            segments=[str(p.relative_to(self.root)) for p in self.segments])


def verified_records(root, report):
    """Bind raw segments to the saved effective config and collector source."""
    from pathlib import Path
    root=Path(root).resolve();provenance=Provenance(**report['provenance'])
    if digest((root/'config.json').read_bytes())!=provenance.config_sha256:
        raise ValueError('raw configuration hash differs')
    source=root/'source'/provenance.source_sha256
    manifest=(source/'manifest.json').read_bytes()
    if digest(manifest)!=provenance.source_sha256:raise ValueError('source manifest hash differs')
    for name,checksum in strict_json(manifest)['files'].items():
        if Path(name).name!=name or digest((source/name).read_bytes())!=checksum:
            raise ValueError('collector source hash differs')
    positions={};seen=set()
    for name in report['segments']:
        path=(root/name).resolve()
        if not path.is_relative_to(root) or path in seen:raise ValueError('foreign or duplicate raw segment')
        seen.add(path)
        for record in iter_segment_records(path):
            if record.provenance!=provenance:raise ValueError('raw provenance differs')
            key=record.receipt.stream_key
            if record.receipt.receive_seq!=positions.get(key,-1)+1:raise ValueError('raw receipt discontinuity')
            positions[key]=record.receipt.receive_seq
            yield record
