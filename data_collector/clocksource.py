"""Read-only local chrony evidence and bounded recurring clock admission.

No NTP implementation, daemon configuration or clock adjustment. Trust in the
installed daemon and its external UTC sources is an intended-host release gate.
"""
import asyncio
import base64
from decimal import Decimal, ROUND_CEILING
import re
import time

from .clock import ClockSample
from .clocklog import SOURCE
from .contracts import RawRecord, Receipt, canonical, digest, integer
from .lane import ArchiveLane, DurableClockAck

MAX_INTEGER = 2**63-1


def tracking_error_ns(body, *, utc_ns, max_reference_age_ns):
    """Absolute, zero-centred error interval from chrony4.8 tracking CSV.

    Direction is deliberately not claimed. Root distance assumes a correct
    stratum-1 source, as documented by chrony; this does not authenticate it.
    """
    integer(utc_ns,'tracking UTC');integer(max_reference_age_ns,'reference age',1)
    if not isinstance(body, bytes) or len(body)>8192:raise ValueError('tracking byte bound')
    lines=body.decode('ascii').splitlines()
    if len(lines)!=1:raise ValueError('one tracking report required')
    fields=lines[0].split(',')
    if (len(fields)!=14 or not re.fullmatch('[0-9A-Fa-f]{8}',fields[0]) or
            fields[0].upper() in {'00000000','7F7F0101'} or not fields[1] or fields[1]=='0.0.0.0' or
            not fields[2].isdigit() or not 1<=int(fields[2])<=15 or fields[13]!='Normal'):
        raise ValueError('external synchronized tracking source required')
    values=[]
    for value in fields[3:13]:
        if not re.fullmatch(r'[+-]?\d{1,20}(?:\.\d{1,20})?(?:[eE][+-]?\d{1,3})?',value):
            raise ValueError('bounded tracking number required')
        number=Decimal(value)
        if not number.is_finite() or abs(number)>Decimal('1e12'):raise ValueError('tracking number range')
        values.append(number)
    reference,correction,last,rms,frequency,residual,skew,delay,dispersion,interval=values
    if min(reference,rms,skew,dispersion,interval)<0:raise ValueError('negative tracking uncertainty')
    age=Decimal(utc_ns)-reference*1_000_000_000
    if not 0<=age<=max_reference_age_ns:raise ValueError('tracking reference stale or future')
    # CSV offsets/delay/dispersion round to9 decimal seconds. Add3ns rather
    # than rounding a positive uncertainty downward.
    bound=(abs(correction)+dispersion+abs(delay)/2)*1_000_000_000
    return min(MAX_INTEGER,int(bound.to_integral_value(rounding=ROUND_CEILING))+3)


async def _settle(future):
    while True:
        try:return await asyncio.shield(future)
        except asyncio.CancelledError:
            if future.done():return future.result()


async def read_tracking(executable,timeout_seconds,max_output_bytes):
    """Bound both pipes; kill/reap owned child before propagating cancellation."""
    spawning=asyncio.create_task(asyncio.create_subprocess_exec(executable,'-c','-h','127.0.0.1','tracking',
        stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,limit=max_output_bytes))
    try:process=await asyncio.shield(spawning)
    except asyncio.CancelledError:
        process=await _settle(spawning)
        if process.returncode is None:
            try:process.kill()
            except ProcessLookupError:pass
        await _settle(asyncio.ensure_future(process.communicate()))
        raise
    async def bounded(stream):
        try:
            await stream.readexactly(max_output_bytes+1)
            raise ValueError('chronyc output exceeds bound')
        except asyncio.IncompleteReadError as exc:return exc.partial
    tasks=[asyncio.create_task(bounded(process.stdout)),asyncio.create_task(bounded(process.stderr)),
           asyncio.create_task(process.wait())]
    io=asyncio.gather(*tasks)
    try:
        async with asyncio.timeout(timeout_seconds):
            out,err,code=await asyncio.shield(io)
        return code,out,err
    finally:
        if process.returncode is None:
            try:process.kill()
            except ProcessLookupError:pass
        for task in tasks:
            if not task.done():task.cancel()
        await _settle(asyncio.gather(*tasks,return_exceptions=True))
        io.cancel()
        try:await _settle(io)
        except (Exception,asyncio.CancelledError):pass
        async def drain(stream):
            while await stream.read(8192):pass
        await _settle(asyncio.gather(drain(process.stdout),drain(process.stderr),process.wait()))


class ChronyClockSampler:
    """One bounded run, one clock-source owner; records failure as unaligned.

    Sample timestamps are taken immediately before FIFO admission, after the
    asynchronous command. Command age is included in the uncertainty interval.
    Caller must stop/drain this task before stopping its raw lane.
    """
    def __init__(self,lane,*,max_samples=100,interval_ns=None,timeout_seconds=5,
                 max_reference_age_ns=300_000_000_000,executable='chronyc',
                 read=None,wait_until=None):
        if not isinstance(lane,ArchiveLane) or lane._clock is None or not lane.health()['accepting']:
            raise ValueError('accepting measured-clock lane required')
        for name,value in (('samples',max_samples),('timeout',timeout_seconds),('reference age',max_reference_age_ns)):
            integer(value,name,1)
        policy=lane._clock.policy
        interval_ns=policy['sample_interval_ns']//2 if interval_ns is None else interval_ns
        integer(interval_ns,'sampling interval',1)
        if (max_samples>10000 or timeout_seconds>30 or interval_ns>policy['max_sample_age_ns']//2 or
                max_reference_age_ns>3_600_000_000_000 or policy['max_offset_ns']>=MAX_INTEGER or
                not isinstance(executable,str) or not executable or len(executable)>4096 or
                (read is not None and not callable(read)) or (wait_until is not None and not callable(wait_until))):
            raise ValueError('bounded clock source configuration required')
        if hasattr(lane,'_reference_sampler'):raise ValueError('one reference sampler owns lane')
        self.lane=lane;self.max_samples=max_samples;self.interval_ns=interval_ns
        self.timeout_seconds=timeout_seconds;self.max_reference_age_ns=max_reference_age_ns;self.executable=executable
        self.read=read or read_tracking;self.wait_until=wait_until or self._sleep_until
        self._stop=asyncio.Event();self.updated=asyncio.Event();self._started=self._terminal=self._cancelled=False
        self.samples=self.failures=0;self.last_ack=None
        lane._reference_sampler=self

    async def _sleep_until(self,deadline):
        await asyncio.sleep(max(0,deadline-time.monotonic_ns())/1_000_000_000)

    def stop(self):self._stop.set()

    async def _owned(self,future):
        while True:
            try:return await asyncio.shield(future)
            except asyncio.CancelledError:
                self._cancelled=True;self.stop()
                if future.done():return future.result()

    async def _sample_once(self):
        start=self.lane._sample();out=err=b'';code=None;error=None;complete=False
        try:
            code,out,err=await self.read(self.executable,self.timeout_seconds,8192)
            if type(code) is not int or type(out) is not bytes or type(err) is not bytes or max(len(out),len(err))>8192:
                code=None;out=err=b'';raise ValueError('chronyc result shape/bound')
            complete=True
        except (OSError,TimeoutError,ValueError) as exc:error=type(exc).__name__
        utc,mono,boot=self.lane._sample()
        if boot!=start[2] or mono<start[1]:raise ValueError('clock source process epoch changed')
        uncertainty=MAX_INTEGER
        try:
            if error is not None or code!=0 or err:raise ValueError('chronyc unsuccessful')
            uncertainty=tracking_error_ns(out,utc_ns=utc,max_reference_age_ns=self.max_reference_age_ns)
            uncertainty=min(MAX_INTEGER,uncertainty+mono-start[1]+abs((utc-start[0])-(mono-start[1])))
        except (ValueError,UnicodeError):error=error or 'tracking_unusable'
        if error is not None:self.failures+=1
        body=canonical(dict(schema='dc.chrony-source/1',command=[self.executable,'-c','-h','127.0.0.1','tracking'],
            start=dict(utc_ns=start[0],monotonic_ns=start[1],boot_id=start[2]),
            completed=dict(utc_ns=utc,monotonic_ns=mono,boot_id=boot),returncode=code,error=error,
            stdout_base64=base64.b64encode(out).decode(),stderr_base64=base64.b64encode(err).decode(),
            output_complete=complete,
            reference_age_limit_ns=self.max_reference_age_ns,uncertainty_ns=uncertainty))
        source=RawRecord(Receipt(self.lane.identity['capture_id'],self.lane.identity['process_session_id'],SOURCE,
            self.lane._clock_submitted,utc,mono,'measurement'),*self.lane.writer.identity,'control',
            'clock_measurement','control',body,self.lane.provenance)
        sample=ClockSample(utc,mono,0,uncertainty,boot,digest(source.to_bytes()))
        ack=await self._owned(self.lane.submit_clock(sample,source))
        if (not isinstance(ack,DurableClockAck) or not ack.timing_valid or
                ack.assessment.evidence_sha256!=sample.evidence_sha256):raise ValueError('clock durability unconfirmed or late')
        self.last_ack=ack;self.samples+=1;self.updated.set()
        return mono

    async def run(self):
        if self._started:raise ValueError('clock sampler can run once')
        self._started=True
        try:
            while not self._stop.is_set() and self.samples<self.max_samples:
                mono=await self._sample_once()
                if self._cancelled:raise asyncio.CancelledError
                if self.samples>=self.max_samples:break
                timer=asyncio.create_task(self.wait_until(mono+self.interval_ns))
                stopped=asyncio.create_task(self._stop.wait())
                try:
                    done,_=await asyncio.wait((timer,stopped),return_when=asyncio.FIRST_COMPLETED)
                    if timer in done:
                        timer.result()
                        if not self._stop.is_set() and self.lane._sample()[1]<mono+self.interval_ns:
                            raise ValueError('clock timer woke early')
                finally:
                    for task in (timer,stopped):
                        if not task.done():task.cancel()
                    await self._owned(asyncio.gather(timer,stopped,return_exceptions=True))
        except asyncio.CancelledError:
            self._cancelled=True;self.stop();raise
        except Exception:
            if self.lane.health()['accepting']:self.lane.halt_snapshot_schedule('snapshot_schedule_clock_invalid')
            raise
        finally:self._terminal=True;self.updated.set()
        if self._cancelled:raise asyncio.CancelledError
        return dict(samples=self.samples,failures=self.failures,terminal=self._terminal)
