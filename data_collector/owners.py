"""Bounded owner-plan cutovers in the existing raw journal.

Plans are frozen before allocation. Each replacement book path starts at the
next durable receipt and needs a fresh native anchor; no old book is merged.
Legacy allocations preceding the first declaration retain legacy semantics.
"""
from dataclasses import asdict,dataclass
import uuid

from .contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,integer,strict_json,text
from .metadata import MetadataVersion
from .replay import SnapshotPlan
from .validity import GapCorrection,LedgerEntry

SCHEMA='dc.owner-plan/2'
PREFIX='dc-owner/1:'


def new_owner_epoch():
    """Fresh namespace for managed owners; legacy free-form IDs stay legacy."""
    return PREFIX+uuid.uuid4().hex


def _key(binding):return binding.target.instrument.key_id,binding.target.profile


def _path(binding):
    return (binding.ws_source,binding.seed_source,binding.ws_start_seq,binding.seed_start_seq,
            binding.depth,binding.signed_prices,binding.native_channel)


def _sources(plan):
    return {s for b in plan.bindings for s in (b.ws_source,b.metadata_source,b.seed_source) if s is not None}


@dataclass(frozen=True,slots=True)
class OwnerPlan:
    record_id: str
    raw_sha256: str
    effective_cutoff_id: int
    plan: SnapshotPlan


class OwnerPlans:
    def __init__(self,writer,*,capture_scope,capture_id,process_session_id,provenance,prior_records=lambda:(),
                 max_records=200000,max_scan_bytes=256*1024*1024,max_plan_bytes=16*1024*1024,
                 max_plans=10000,max_sources=100000,max_metadata_bytes=8*1024*1024):
        for name,value in (('scope',capture_scope),('capture',capture_id),('process',process_session_id)):
            text(value,name)
            if len(value.encode())>128:raise ValueError('owner identity byte bound')
        if not isinstance(provenance,Provenance) or not callable(prior_records):raise ValueError('typed owner provenance/factory required')
        for name,value in (('records',max_records),('scan bytes',max_scan_bytes),('plan bytes',max_plan_bytes),
                           ('plans',max_plans),('sources',max_sources),('metadata bytes',max_metadata_bytes)):
            integer(value,name,1)
        self.writer,self.scope,self.capture_id,self.session=writer,capture_scope,capture_id,process_session_id
        self.provenance,self.prior_records=provenance,prior_records
        self.max_records,self.max_scan_bytes=max_records,max_scan_bytes
        self.max_plan_bytes,self.max_plans,self.max_sources=max_plan_bytes,max_plans,max_sources
        self.max_metadata_bytes=max_metadata_bytes
        self.refresh()

    @property
    def connection(self):return 'control:owners:'+self.scope

    @property
    def channel(self):return 'owner_plans:'+self.scope

    def _records(self):
        yield from self.prior_records()
        for encoded in self.writer.encoded_records(allow_unsealed=True):yield RawRecord.from_bytes(encoded)

    def _scan_records(self):
        count=size=0
        for record in self._records():
            count+=1;size+=len(record.to_bytes())+64
            if count>self.max_records or size>self.max_scan_bytes:raise ValueError('owner scan bound; checkpoint required')
            yield record

    def _check_plan(self,plan,process,config):
        if not isinstance(plan,SnapshotPlan) or len(plan.bindings)>256:raise ValueError('bounded typed owner plan required')
        if plan.config_sha256!=config:raise ValueError('owner plan config differs from envelope')
        for binding in plan.bindings:
            if (binding.target.instrument.venue,binding.target.instrument.environment)!=self.writer.identity:
                raise ValueError('owner plan venue differs')
            for stream in (binding.ws_source,binding.seed_source,binding.metadata_source):
                if stream is None:continue
                if stream.capture_id!=self.capture_id or stream.process_session_id!=process:
                    raise ValueError('active owner sources require current process/capture')
                if stream.connection_id.startswith('control:'):raise ValueError('native owner source cannot be control stream')
                if stream in self.configs and self.configs[stream]!=config:
                    raise ValueError('changed config requires fresh source identity')
            metadata=self.metadata.get(binding.metadata_version_id)
            if metadata is None or metadata.instrument!=binding.target.instrument or metadata.receipt.stream_key!=binding.metadata_source:
                raise ValueError('owner metadata must already be durable on its declared source')

    def _transition(self,plan,process,config):
        self._check_plan(plan,process,config)
        old={} if not self.plans else {_key(b):b for b in self.plans[-1].plan.bindings}
        for binding in plan.bindings:
            key=_key(binding);previous=old.get(key);epoch=binding.target.owner_epoch
            attributes=self.metadata[binding.metadata_version_id].attributes_json
            unchanged=(previous is not None and previous.target.owner_epoch==epoch and _path(previous)==_path(binding) and
                self.plans[-1].plan.config_sha256==plan.config_sha256 and
                self.metadata[previous.metadata_version_id].attributes_json==attributes)
            if unchanged:continue
            if (not epoch.startswith(PREFIX) or len(epoch)!=len(PREFIX)+32 or
                    any(c not in '0123456789abcdef' for c in epoch[len(PREFIX):])):
                raise ValueError('fresh managed owner epoch required; use new_owner_epoch')
            if (key,epoch) in self.used:raise ValueError('owner epoch reused or native binding changed without reset')
            for stream,start in ((binding.ws_source,binding.ws_start_seq),(binding.seed_source,binding.seed_start_seq)):
                if stream is not None and any(stream in streams for streams in self.open_allocations.values()):
                    raise ValueError('await cutoff resolution before reusing a source for new owner')
                if stream is not None and start!=self.positions.get(stream,-1)+1:
                    raise ValueError('new owner must start at next durable receipt')
        if len(self.used|{(_key(b),b.target.owner_epoch) for b in plan.bindings})>self.max_sources:raise ValueError('owner identity projection bound')

    def _allocation(self,record):
        value=strict_json(record.body)
        if value.get('kind') in {'closed','failed'}:
            identity=value['payload']['cutoff_id']
            if identity not in self.open_allocations:raise ValueError('owner resolution allocation missing')
            self.open_allocations.pop(identity)
            return
        if value.get('kind')!='allocated':return
        payload=value['payload'];identity=payload['cutoff_id']
        if identity!=len(self.allocations):raise ValueError('owner allocation prefix missing')
        selected=len(self.plans)-1 if self.plans else None
        if selected is not None:
            plan=self.plans[selected].plan
            self._check_plan(plan,record.receipt.process_session_id,record.provenance.config_sha256)
            if payload['dependencies']!=plan.dependencies or {canonical(s) for s in payload['required_streams']}!={canonical(asdict(s)) for s in _sources(plan)}:
                raise ValueError('allocation differs from frozen owner plan/sources')
        self.allocations.append(selected)
        self.open_allocations[identity]={StreamKey(**s) for s in payload['required_streams']}

    def refresh(self,*,for_declaration=False):
        metadata_required=for_declaration or any(r.channel==self.channel or r.receipt.connection_id==self.connection for r in self._scan_records())
        self.plans=[];self.allocations=[];self.open_allocations={};self.positions={};self.configs={};self.metadata={};self.used=set()
        self.head=None;self.bytes=0;metadata_bytes=0
        from .snapshots import plan_from_descriptor
        for record in self._scan_records():
            r=record.receipt
            if record.channel==self.channel or r.connection_id==self.connection:
                value=strict_json(record.body)
                if ((r.capture_id,r.connection_id,record.channel,record.kind,record.native_product,record.instrument,record.native_fields_json)!=
                    (self.capture_id,self.connection,self.channel,'control','control',None,b'{}') or
                    (record.venue,record.environment)!=self.writer.identity or
                    r.receive_seq!=self.positions.get(r.stream_key,-1)+1 or not isinstance(value,dict) or
                    set(value)!={'schema','scope','sequence','previous_sha256','effective_cutoff_id','plan'} or
                    value['schema']!=SCHEMA or value['scope']!=self.scope or value['sequence']!=len(self.plans) or
                    value['previous_sha256']!=self.head or value['effective_cutoff_id']!=len(self.allocations)):
                    raise ValueError('owner declaration envelope/prefix/cutoff differs')
                if len(self.plans)>=self.max_plans or len(record.body)>1024*1024 or self.bytes+len(record.body)>self.max_plan_bytes:
                    raise ValueError('owner plan projection bound')
                plan=plan_from_descriptor(value['plan']);self._transition(plan,r.process_session_id,record.provenance.config_sha256)
                self.plans.append(OwnerPlan(record.record_id,digest(record.to_bytes()),len(self.allocations),plan))
                self.used.update((_key(b),b.target.owner_epoch) for b in plan.bindings)
                self.head=digest(record.to_bytes());self.bytes+=len(record.body)
            elif record.channel=='cutoffs:'+self.scope:
                self._allocation(record)
            elif metadata_required and record.kind=='metadata_version' and record.instrument is not None:
                value=strict_json(record.body)
                if value.get('schema')!='dc.metadata/2':raise ValueError('unknown owner metadata schema')
                metadata=MetadataVersion(record.instrument,r,value['effective_at_ns'],canonical(value['attributes']),value['supersedes'])
                metadata_bytes+=len(record.body)
                if metadata_bytes>self.max_metadata_bytes:raise ValueError('owner metadata byte bound')
                self.metadata[metadata.version_id]=metadata
            elif metadata_required and record.channel=='validity:'+self.scope and record.kind=='coverage_event':
                payload=LedgerEntry.from_bytes(record.body).payload
                if isinstance(payload,GapCorrection):
                    self.used.add(((payload.target.instrument.key_id,payload.target.profile),payload.target.owner_epoch))
                    if len(self.used)>self.max_sources:raise ValueError('owner identity projection bound')
            if r.stream_key not in self.positions and len(self.positions)>=self.max_sources:raise ValueError('owner source projection bound')
            self.positions[r.stream_key]=r.receive_seq
            if r.stream_key not in self.configs:self.configs[r.stream_key]=record.provenance.config_sha256
            elif self.configs[r.stream_key]!=record.provenance.config_sha256:self.configs[r.stream_key]=None

    def check_next_allocation(self,dependencies,streams):
        self.refresh()
        if not self.plans:return # explicit legacy mode until first declaration
        plan=self.plans[-1].plan;self._check_plan(plan,self.session,self.provenance.config_sha256)
        if dependencies!=plan.dependencies or set(streams)!=_sources(plan):raise ValueError('allocation differs from active owner plan')

    def declare(self,plan,*,utc_ns,monotonic_ns,clock_epoch):
        self.refresh(for_declaration=True);self._check_plan(plan,self.session,self.provenance.config_sha256)
        if self.plans and self.plans[-1].plan==plan:return self.plans[-1]
        self._transition(plan,self.session,self.provenance.config_sha256)
        r=Receipt(self.capture_id,self.session,self.connection,
            self.positions.get(StreamKey(self.capture_id,self.session,self.connection),-1)+1,
            utc_ns,monotonic_ns,clock_epoch)
        body=canonical(dict(schema=SCHEMA,scope=self.scope,sequence=len(self.plans),previous_sha256=self.head,
            effective_cutoff_id=len(self.allocations),plan=asdict(plan)))
        record=RawRecord(r,*self.writer.identity,'control',self.channel,'control',body,self.provenance)
        if (len(self.plans)>=self.max_plans or len(body)>1024*1024 or self.bytes+len(body)>self.max_plan_bytes or
                len(record.to_bytes())>self.writer.limits.max_record_bytes):raise ValueError('owner declaration byte/count bound')
        self.writer.append(record)
        self.refresh()
        return self.plans[-1]

    def proof_for(self,cutoff_id,plan):
        selected=self.allocations[cutoff_id]
        if selected is None:return ()
        if self.plans[selected].plan!=plan:raise ValueError('snapshot differs from allocated owner plan')
        return tuple(dict(record_id=p.record_id,raw_sha256=p.raw_sha256) for p in self.plans[:selected+1])
