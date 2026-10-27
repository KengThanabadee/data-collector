"""Bounded Deribit socket ingress over the existing durable lane and scheduler.

The caller supplies an aiohttp-compatible HTTP session, samples clock health,
and owns the snapshot pipeline. This receiver opens and owns its bounded socket
until close. It stops on resync/fault; reconnect and lifecycle supervision remain
separate. No book state is applied before exact raw durability acknowledgement.
"""
import asyncio
from dataclasses import asdict,dataclass,replace
import uuid

import aiohttp

from .books import SnapshotBook
from .contracts import RawRecord,Receipt,StreamKey,canonical,digest,integer
from .decoders import native_json,deribit_session_end
from .lane import DurableAck,DurableGapAck,PENDING_CLOCK_EPOCH
from .owners import new_owner_epoch
from .quota import Priority,PriorityQuota
from .replay import _book_message
from .schedule import MinuteCutoffs


class DeribitStopped(RuntimeError):
    pass


@dataclass(frozen=True,slots=True)
class DeribitLimits:
    max_frames: int = 64
    max_bytes: int = 20*1024*1024
    max_message_bytes: int = 2*1024*1024
    max_seconds: int = 90
    ack_timeout_seconds: int = 10
    bootstrap_timeout_seconds: int = 30
    idle_timeout_seconds: int = 30
    max_total_levels: int = 100000

    def __post_init__(self):
        for name,value in asdict(self).items():integer(value,name,1)
        if self.max_frames>100000 or self.max_seconds>7200 or self.max_message_bytes>2*1024*1024:
            raise ValueError('bounded Deribit receiver profile required')


class DeribitBookSession:
    """One frozen public book group, up to50 channels on one owned socket.

    Call run once with an HTTP session. For an allocated cutoff, use
    healthy_streams_for_plan to avoid attributing this source to another owner.
    No transport fault is misrepresented as a native sequence gap.
    """
    def __init__(self,schedule,quota,*,control_charges,limits=DeribitLimits()):
        if not isinstance(schedule,MinuteCutoffs) or not isinstance(quota,PriorityQuota) or quota.venue!='deribit':
            raise ValueError('Deribit schedule and shared quota required')
        if not isinstance(limits,DeribitLimits):raise ValueError('typed Deribit limits required')
        original=schedule.plan()
        if not 1<=len(original.bindings)<=50 or schedule.lane.writer.identity not in {('deribit','production'),('deribit','testnet')}:
            raise ValueError('bounded local Deribit book group required')
        if any(b.target.profile!='deribit_full' or b.seed_source is not None for b in original.bindings):
            raise ValueError('Deribit snapshot book bindings required')
        if len({b.target.instrument.native_product for b in original.bindings})!=1:
            raise ValueError('one explicit native product per receiver group required')
        self.product=original.bindings[0].target.instrument.native_product
        if len({b.native_channel for b in original.bindings})!=len(original.bindings):raise ValueError('distinct native channels required')
        if not isinstance(control_charges,dict) or not control_charges or len(control_charges)>32:
            raise ValueError('bounded shared control charges required')
        for name,cost in control_charges.items():
            integer(cost,'control cost',1)
            if name not in quota.pools or cost>quota.pools[name].rule.capacity:raise ValueError('known admissible control quota required')
        if hasattr(schedule,'_deribit_receiver'):raise ValueError('one receiver owns this schedule plan')
        self.schedule,self.lane,self.quota=schedule,schedule.lane,quota
        self.charges,self.limits=dict(control_charges),limits
        identity=self.lane.identity
        self.source=StreamKey(identity['capture_id'],identity['process_session_id'],'deribit:'+uuid.uuid4().hex)
        self.plan=replace(original,bindings=tuple(replace(b,ws_source=self.source,ws_start_seq=0,
            target=replace(b.target,owner_epoch=new_owner_epoch())) for b in original.bindings))
        self.models=[SnapshotBook(b.target.instrument,b.target.profile,signed_prices=b.signed_prices) for b in self.plan.bindings]
        self._channels={b.native_channel:i for i,b in enumerate(self.plan.bindings)}
        self._stop=asyncio.Event();self._started=self._terminal=self._cancelled=False
        self._seq=self._frames=self._bytes=self._controls=0
        self._acked=False;self._failed=False;self.reason=None;self.gaps=[];self.ends=[];self._end_record=None
        self._start_ns=self._last_receive_ns=None
        envelope=RawRecord(Receipt(self.source.capture_id,self.source.process_session_id,self.source.connection_id,
            2**63-1,2**63-1,2**63-1,self.source.process_session_id+':clock:1023'),
            *self.lane.writer.identity,self.product,'public/set_heartbeat','control',b'',self.lane.provenance,None,
            canonical(dict(direction='outbound',collector_ingress_ordinal=2**63-1)))
        maximum=4*((limits.max_message_bytes+2)//3)+len(envelope.to_bytes())
        if maximum>min(self.lane.writer.limits.max_record_bytes,self.lane.queue.limits.max_item_bytes) or maximum+4608>self.lane.queue.limits.max_bytes:
            raise ValueError('native message envelope exceeds archive queue/writer bounds')
        self._proof_budget()
        schedule._deribit_receiver=self

    def _proof_budget(self):
        # Each frame may require one outbound test response. All cutoff controls
        # are retained by native mapping, including older closed cutoffs.
        clock=self.lane.health()['clock']
        # Terminal policy also requires the managed declaration chain and its
        # metadata. Prior raw count conservatively bounds recovered prerequisites.
        owner_proof=self.lane._source_records+51*self.lane._owner_plan_requests+len(self.plan.bindings)+1
        dependencies=2*self.limits.max_frames+6+2*(clock['cutoff_allocations_submitted']+self.schedule.max_cutoffs)+owner_proof
        if dependencies>256 or any(len(canonical(asdict(b.target)))+6000+dependencies*200>65536 for b in self.plan.bindings):
            raise ValueError('native proof headroom requires shorter capture or checkpoints')
        # Leave conservative room for raw envelopes, clock/cutoff/publication
        # controls and one audit/correction per selected binding.
        records=2*self.limits.max_frames+64*len(self.models)+40*self.schedule.max_cutoffs+32
        size=2*(self.limits.max_bytes+self.limits.max_message_bytes)+records*4096+len(canonical(asdict(self.plan)))*2
        if (self.lane._source_records+self.lane.writer.state.records+records>self.lane.snapshot_limits.max_records or
                self.lane._source_bytes+self.lane.writer.state.valid_bytes+size>self.lane.snapshot_limits.max_scan_bytes):
            raise ValueError('native replay scan headroom requires checkpoints')

    def stop(self):self._stop.set()

    async def _owned(self,future):
        while True:
            try:return await asyncio.shield(future)
            except asyncio.CancelledError:
                self._cancelled=True;self._stop.set();self.schedule.stop()
                if future.done():return future.result()

    def _admit(self,body,channel,kind,*,fields=None):
        utc,mono,_=self.lane._sample()
        record=RawRecord(Receipt(self.source.capture_id,self.source.process_session_id,self.source.connection_id,
            self._seq,utc,mono,PENDING_CLOCK_EPOCH),*self.lane.writer.identity,self.product,channel,kind,body,
            self.lane.provenance,None,canonical(fields or {}))
        future=self.lane.submit_received(record);self._seq+=1
        return record,future

    async def _archive(self,body,channel,kind,*,fields=None):
        return await self._ack(*self._admit(body,channel,kind,fields=fields))

    async def _ack(self,record,future):
        ack=await self._owned(future)
        if not isinstance(ack,DurableAck) or not ack.timing_valid or ack.record_id!=record.record_id:
            raise DeribitStopped('raw durability unconfirmed or late')
        # Native decoders need the exact saved receipt epoch. The raw body stays
        # untouched; ordinal/hash belong to the disk acknowledgement.
        stored=replace(record,receipt=replace(record.receipt,clock_epoch=ack.clock_epoch),
            native_fields_json=canonical(dict(native_json(record.native_fields_json),collector_ingress_ordinal=ack.ingress_ordinal)))
        if digest(stored.to_bytes())!=ack.raw_sha256:raise DeribitStopped('raw acknowledgement hash differs')
        return stored

    async def _send(self,socket,request):
        _,mono,_=self.lane._sample()
        await self.quota.acquire(self.charges,priority=Priority.RECOVERY,
            deadline_ns=mono+self.limits.ack_timeout_seconds*1_000_000_000)
        body=canonical(request)
        await self._archive(body,request['method'],'control',fields=dict(direction='outbound'))
        await asyncio.wait_for(socket.send_str(body.decode()),self.limits.ack_timeout_seconds)
        self._controls+=1

    async def _receive(self,socket,timeout):
        async def read():
            message=await socket.receive()
            if message.type in {aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY}:
                body=message.data.encode('utf-8') if message.type==aiohttp.WSMsgType.TEXT else message.data
                record,future=self._admit(body,'multiplex','ws_message')
                self._last_receive_ns=record.receipt.monotonic_ns
            else:
                record,future=self._admit(canonical(dict(event='socket_event',type=int(message.type))),
                    'deribit_session','control')
            # Receipt and FIFO admission share the socket-return event-loop turn.
            return message,record,future
        receive=asyncio.create_task(read());stop=asyncio.create_task(self._stop.wait())
        try:
            done,_=await asyncio.wait((receive,stop),timeout=timeout,return_when=asyncio.FIRST_COMPLETED)
            if receive in done:return receive.result()
            if stop in done:return None
            raise TimeoutError('Deribit receive deadline')
        except asyncio.CancelledError:
            self._cancelled=True;self._stop.set();self.schedule.stop()
            # A delivery already admitted before cancellation still needs its
            # acknowledgement and native failure classification.
            if receive.done() and not receive.cancelled():return receive.result()
            return None
        finally:
            for task in (receive,stop):
                if not task.done():task.cancel()
            await self._owned(asyncio.gather(receive,stop,return_exceptions=True))

    def healthy_streams_for_plan(self,plan):
        # Metadata was checked durable at declaration; its observation age is
        # independent of socket continuity. A stopped socket certifies no books.
        if plan!=self.plan:return ()
        streams={b.metadata_source for b in plan.bindings}
        now=self.lane._sample()[1]
        timely=(self._last_receive_ns is not None and self._start_ns is not None and
            self._last_receive_ns<=now<self._last_receive_ns+self.limits.idle_timeout_seconds*1_000_000_000 and
            now<self._start_ns+self.limits.max_seconds*1_000_000_000)
        if timely and self._acked and not self._failed and not self._stop.is_set() and all(m.state=='live' for m in self.models):
            streams.add(self.source)
        return tuple(sorted(streams,key=lambda s:s.connection_id))

    async def _apply(self,record,value):
        selected=[]
        try:
            params={} if value is None else value.get('params',{})
            channel=params.get('channel') if isinstance(params,dict) else None
            # Parse one native book once; subscription/control errors or malformed
            # routing envelopes are checked against every binding, as in replay.
            global_error=value is not None and ('error' in value or value.get('success') is False)
            indices=([self._channels[channel]] if not global_error and isinstance(channel,str) and channel in self._channels else
                range(len(self.models)))
            for index in indices:
                model=self.models[index]
                try:
                    event=_book_message(record,self.plan.bindings[index])
                    outcome='control' if event is None else model.receive(event)
                except ValueError:outcome=model.invalidate('native_decode_or_apply_failed')
                if outcome=='invalid':selected.append(index)
        finally:
            if sum(m.book.level_count for m in self.models)>self.limits.max_total_levels:
                raise DeribitStopped('aggregate native book level bound')
        if selected:
            self._failed=True;self.schedule.stop()
            for index in selected:
                utc,mono,_=self.lane._sample()
                ack=await self._owned(self.lane.submit_native_gap(self.plan,index,record.record_id,utc_ns=utc,monotonic_ns=mono))
                if not isinstance(ack,DurableGapAck) or not ack.timing_valid or ack.mapping is None:
                    raise DeribitStopped('native gap correction unconfirmed')
                self.gaps.append(ack.mapping)
            raise DeribitStopped('native_resync_required')

    async def run(self,session):
        if self._started:raise DeribitStopped('receiver can run once')
        self._started=True;failure=None;socket=None
        try:
            self._proof_budget()
            await self.schedule.replace_plan(self.plan)
            endpoint=('wss://www.deribit.com/ws/api/v2' if self.lane.writer.identity[1]=='production' else
                'wss://test.deribit.com/ws/api/v2')
            socket=await asyncio.wait_for(session.ws_connect(endpoint,max_msg_size=self.limits.max_message_bytes,
                autoping=True,compress=0),self.limits.ack_timeout_seconds)
            start=self.lane._sample()[1];last=start;bootstrap=start;self._start_ns=start
            await self._archive(canonical(dict(schema='dc.deribit-session/2',event='connected')),'deribit_session','control')
            await self._send(socket,dict(jsonrpc='2.0',id=1,method='public/subscribe',params=dict(channels=list(self._channels))))
            await self._send(socket,dict(jsonrpc='2.0',id=2,method='public/set_heartbeat',params=dict(interval=10)))
            while not self._stop.is_set():
                if self._frames>=self.limits.max_frames:
                    self.reason='session_frame_bound';break
                now=self.lane._sample()[1]
                deadlines=[start+self.limits.max_seconds*1_000_000_000,last+self.limits.idle_timeout_seconds*1_000_000_000]
                if not self._acked:deadlines.append(start+self.limits.ack_timeout_seconds*1_000_000_000)
                if any(m.state!='live' for m in self.models):deadlines.append(bootstrap+self.limits.bootstrap_timeout_seconds*1_000_000_000)
                if now>=min(deadlines):raise DeribitStopped('session_or_bootstrap_deadline')
                delivery=await self._receive(socket,(min(deadlines)-now)/1_000_000_000)
                if delivery is None:break
                message,record,future=delivery
                record=await self._ack(record,future)
                if message.type not in {aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY}:
                    if deribit_session_end(record) is not None:self._end_record=record
                    raise DeribitStopped('socket_closed_or_error')
                body=record.body
                self._frames+=1;self._bytes+=len(body);last=record.receipt.monotonic_ns
                try:value=native_json(body,max_bytes=self.limits.max_message_bytes)
                except ValueError:
                    await self._apply(record,None)
                    raise DeribitStopped('native JSON rejected')
                if value.get('method')=='heartbeat':
                    params=value.get('params')
                    if isinstance(params,dict) and params.get('type')=='test_request':
                        await self._send(socket,dict(jsonrpc='2.0',id=100+self._frames,method='public/test',params={}))
                if 'error' in value or value.get('success') is False:await self._apply(record,value)
                if type(value.get('id')) is int and value['id']==1:
                    if ('error' in value or not isinstance(value.get('result'),list) or
                            len(value['result'])!=len(self._channels) or any(not isinstance(c,str) for c in value['result']) or
                            set(value['result'])!=set(self._channels)):
                        raise DeribitStopped('subscription_ack_rejected_or_incomplete')
                    self._acked=True
                await self._apply(record,value)
                if self.lane._sample()[1]-record.receipt.monotonic_ns>5_000_000_000:
                    raise DeribitStopped('receive_apply_deadline')
                if self._bytes>self.limits.max_bytes:raise DeribitStopped('session_byte_bound')
            self.reason=self.reason or ('receiver_cancelled' if self._cancelled else 'requested_stop')
        except asyncio.CancelledError:
            self._cancelled=True;failure=DeribitStopped('receiver_cancelled');self.reason='receiver_cancelled'
        except Exception as exc:
            failure=exc;self.reason=str(exc)[:256]
        finally:
            self._stop.set();self.schedule.stop()
            if failure is not None:self._failed=True
            try:
                if self.lane.health()['accepting']:
                    stopped=await self._archive(canonical(dict(schema='dc.deribit-session/2',event='stopped',reason=self.reason,
                        frames=self._frames,bytes=self._bytes,corrections=len(self.gaps))), 'deribit_session','control')
                    end=self._end_record or stopped
                    # A native reset can recover on an open socket; a terminated
                    # connection is an independent cause that cannot be healed.
                    for index in range(len(self.models)):
                        utc,mono,_=self.lane._sample()
                        ack=await self._owned(self.lane.submit_deribit_end(self.plan,index,end.record_id,
                            utc_ns=utc,monotonic_ns=mono))
                        if not isinstance(ack,DurableGapAck) or not ack.timing_valid:
                            raise DeribitStopped('connection end correction unconfirmed')
                        if ack.mapping is not None:self.ends.append(ack.mapping)
            except Exception as exc:
                if failure is None:failure=exc
            if failure is not None and self.lane.health()['accepting']:
                self.lane.halt_snapshot_schedule('snapshot_schedule_failure')
            try:
                if socket is not None:
                    await self._owned(asyncio.ensure_future(asyncio.wait_for(socket.close(),self.limits.ack_timeout_seconds)))
            except Exception as exc:
                if failure is None:failure=exc;self.reason='socket_close_unconfirmed'
                if self.lane.health()['accepting']:self.lane.halt_snapshot_schedule('snapshot_schedule_failure')
            finally:self._terminal=True
        if self._cancelled:raise asyncio.CancelledError
        if failure is not None:raise failure
        return self.health()

    def health(self):
        return dict(started=self._started,terminal=self._terminal,reason=self.reason,acked=self._acked,
            frames=self._frames,bytes=self._bytes,controls=self._controls,corrections=len(self.gaps),
            connection_end_corrections=len(self.ends),
            source=asdict(self.source),books=tuple(m.view() for m in self.models))
