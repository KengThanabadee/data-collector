"""Durable correction ledger over verified raw records and the owned WAL.

Serialized owner API. A pin is exposed only after fsync and projection update.
Prior records must come from verified immutable segments, in original order.
Bounded full scans are intentionally explicit until checkpoint/index integration.
"""
from dataclasses import dataclass

from .contracts import InstrumentKey, Provenance, RawRecord, Receipt, StreamKey, digest, integer, text
from .validity import CloseGap, GapCorrection, LedgerEntry, LedgerPin, LedgerStart, ValidityLedger


@dataclass(frozen=True, slots=True)
class Evidence:
    record_id: str
    raw_sha256: str
    capture_id: str
    venue: str
    environment: str
    instrument: InstrumentKey | None
    native_product: str


class DurableLedger:
    def __init__(self, journal, *, ledger_id, capture_scope, capture_id, process_session_id,
                 provenance, prior_records=lambda: (), max_entries=10000,
                 max_ledger_bytes=16*1024*1024, max_evidence_records=100000,
                 max_scan_records=200000, max_scan_bytes=256*1024*1024, expected_pin=None):
        for name,value in [('ledger_id',ledger_id),('capture_scope',capture_scope),
                           ('capture_id',capture_id),('process_session_id',process_session_id)]:
            text(value,name)
        if not isinstance(provenance,Provenance) or not callable(prior_records):
            raise ValueError('typed provenance and replayable verified prior-record factory required')
        for name,value in [('max_evidence_records',max_evidence_records),('max_scan_records',max_scan_records),
                           ('max_scan_bytes',max_scan_bytes)]:integer(value,name,1)
        self.journal,self.ledger_id,self.scope,self.capture_id,self.session=journal,ledger_id,capture_scope,capture_id,process_session_id
        self.provenance,self.prior_records=provenance,prior_records
        self.max_evidence_records,self.max_scan_records,self.max_scan_bytes=max_evidence_records,max_scan_records,max_scan_bytes
        self._projection_limits=dict(max_entries=max_entries,max_bytes=max_ledger_bytes)
        self._projection=ValidityLedger(ledger_id,**self._projection_limits)
        self._evidence,self._positions,self._control_ids={},{},{}
        if expected_pin is not None and not isinstance(expected_pin,LedgerPin):
            raise ValueError('typed expected restore pin required')
        self._known_pin=expected_pin
        self._scan_records=self._scan_bytes=0
        self.poisoned=False
        self.refresh()

    @property
    def channel(self):return 'validity:'+self.scope

    @property
    def connection(self):return 'control:validity:'+self.ledger_id

    def _all_records(self):
        yield from self.prior_records()
        for encoded in self.journal.encoded_records(allow_unsealed=True):
            yield RawRecord.from_bytes(encoded)

    def _evidence_value(self,record):
        return Evidence(record.record_id,digest(record.to_bytes()),record.receipt.capture_id,record.venue,
                        record.environment,record.instrument,record.native_product)

    def _check_evidence_capacity(self,evidence):
        previous=self._evidence.get(evidence.record_id)
        if previous is not None and previous != evidence:
            raise ValueError('raw record identity has conflicting evidence')
        if previous is None and len(self._evidence)>=self.max_evidence_records:
            raise ValueError('evidence projection bound; checkpoint/index required')
        return previous is None

    def _references(self,payload):
        if isinstance(payload,LedgerStart):return ()
        if isinstance(payload,GapCorrection):return (payload.proof_record_id,payload.evidence_record_id)
        return (payload.recovery_anchor_record_id,payload.evidence_record_id)

    def _check_payload(self,payload):
        if isinstance(payload,(LedgerStart,GapCorrection)) and payload.capture_scope != self.scope:
            raise ValueError('correction scope mismatch')
        if not self._projection.entry_count and not isinstance(payload,LedgerStart):
            raise ValueError('durable ledger requires explicit start')
        target=payload.target if isinstance(payload,GapCorrection) else (
            self._projection.correction(payload.correction_id).target if isinstance(payload,CloseGap) else None)
        references=self._references(payload)
        for identity in references:
            evidence=self._evidence.get(identity)
            if evidence is None or evidence.capture_id != self.capture_id:
                raise ValueError('correction evidence missing from verified durable prefix')
        if references:
            anchor=self._evidence[references[0]]
            if (anchor.venue,anchor.environment)!=(target.instrument.venue,target.instrument.environment):
                raise ValueError('proof/recovery anchor venue differs from target')
            if anchor.instrument is not None and anchor.instrument != target.instrument:
                raise ValueError('proof/recovery anchor instrument differs from target')
            if anchor.native_product not in {target.instrument.native_product,'control'}:
                raise ValueError('proof/recovery anchor product differs from target')

    def _restore_control(self,record):
        if record.channel != self.channel or record.kind != 'coverage_event':return
        r=record.receipt
        if (r.capture_id,r.connection_id)!=(self.capture_id,self.connection) or (
                record.venue,record.environment)!=self.journal.identity:
            raise ValueError('foreign correction control identity')
        if r.receive_seq != self._positions.get(r.stream_key,-1)+1:
            raise ValueError('correction control receipt prefix missing/conflicting')
        entry=LedgerEntry.from_bytes(record.body)
        if entry.sequence != self._projection.entry_count:
            raise ValueError('correction ledger prefix missing/duplicated')
        self._projection.check(entry)
        self._check_payload(entry.payload)
        self._projection.add(entry)
        self._positions[r.stream_key]=r.receive_seq
        self._control_ids[entry.sequence]=record.record_id

    def refresh(self):
        if self.poisoned:raise ValueError('ledger must be reopened after failure')
        # Revalidate the entire supplied prefix, including its order and evidence
        # availability. Never certify a cached pin after a dependency disappears.
        self._projection=ValidityLedger(self.ledger_id,**self._projection_limits)
        self._evidence,self._positions,self._control_ids={},{},{}
        count=size=0
        try:
            for record in self._all_records():
                if not isinstance(record,RawRecord):raise ValueError('typed verified raw records required')
                count+=1;size+=len(record.to_bytes())
                if count>self.max_scan_records or size>self.max_scan_bytes:
                    raise ValueError('ledger scan bound; checkpoint/index required')
                evidence=self._evidence_value(record)
                if not self._check_evidence_capacity(evidence):continue
                # A correction may only reference evidence already observed in
                # the durable prefix, not its own control or a later record.
                self._restore_control(record)
                self._evidence[evidence.record_id]=evidence
            if self._known_pin is not None:
                self._projection.verify_pin(self._known_pin)
            if self._projection.entry_count:
                self._known_pin=self._projection.pin()
            self._scan_records,self._scan_bytes=count,size
        except BaseException:
            self.poisoned=True
            raise

    def _append(self,payload,*,utc_ns,monotonic_ns,clock_epoch):
        self.refresh()
        count=self._projection.entry_count
        entry=LedgerEntry(self.ledger_id,count,self._projection.pin().head_hash if count else None,payload)
        self._projection.check(entry)
        self._check_payload(payload)
        stream=StreamKey(self.capture_id,self.session,self.connection)
        receipt=Receipt(self.capture_id,self.session,self.connection,self._positions.get(stream,-1)+1,
                        utc_ns,monotonic_ns,clock_epoch)
        record=RawRecord(receipt,*self.journal.identity,'control',self.channel,'coverage_event',entry.to_bytes(),self.provenance)
        evidence=self._evidence_value(record)
        self._check_evidence_capacity(evidence)
        if self._scan_records+1>self.max_scan_records or self._scan_bytes+len(record.to_bytes())>self.max_scan_bytes:
            raise ValueError('proposed control exceeds ledger scan bound; checkpoint/index required')
        self.poisoned=True
        self.journal.append(record)
        self._projection.add(entry)
        self._evidence[evidence.record_id]=evidence
        self._positions[stream]=receipt.receive_seq
        self._control_ids[entry.sequence]=record.record_id
        self._scan_records+=1;self._scan_bytes+=len(record.to_bytes())
        self._known_pin=self._projection.pin()
        self.poisoned=False
        return self._projection.pin()

    def initialize(self,*,utc_ns,monotonic_ns,clock_epoch):
        self.refresh()
        if self._projection.entry_count:return self._projection.pin()
        return self._append(LedgerStart(self.scope),utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)

    def append(self,payload,*,utc_ns,monotonic_ns,clock_epoch):
        if not isinstance(payload,(GapCorrection,CloseGap)):
            raise ValueError('explicit gap/recovery payload required')
        return self._append(payload,utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)

    def pin(self):
        self.refresh()
        return self._projection.pin()

    def assess(self,capture_scope,target,cutoff_id,validity_as_published,pin):
        self.refresh()
        if capture_scope != self.scope:raise ValueError('assessment scope mismatch')
        return self._projection.assess(capture_scope,target,cutoff_id,validity_as_published,pin)

    def entries(self,pin):
        self.refresh()
        return self._projection.entries(pin)

    def dependencies(self,pin):
        """Exact immutable raw hashes required for this pinned ledger knowledge."""
        self.refresh()
        identities=set()
        for entry in self._projection.entries(pin):
            identities.add(self._control_ids[entry.sequence])
            identities.update(self._references(entry.payload))
        return tuple(self._evidence[identity] for identity in sorted(identities))
