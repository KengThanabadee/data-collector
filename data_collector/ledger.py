"""Durable correction ledger over verified raw records and the owned WAL.

Serialized owner API. A pin is exposed only after fsync and projection update.
Prior records must come from verified immutable segments, in original order.
Bounded full scans are intentionally explicit until checkpoint/index integration.
"""
from dataclasses import dataclass

from .contracts import InstrumentKey, Provenance, RawRecord, Receipt, StreamKey, digest, integer, sha256, strict_json, text
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
        self._max_mapping_bytes=max_ledger_bytes
        self.poisoned=False
        self.refresh(allow_pending_mappings=True)

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
        if isinstance(payload,GapCorrection) and payload.proof_record_id in self._mappings:
            from .gapmap import mapping_payload
            if payload!=mapping_payload(self._mappings[payload.proof_record_id],payload.proof_record_id):
                raise ValueError('correction differs from native mapping audit')
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

    def _restore_mapping(self,record):
        channel='gap_mapping:'+self.scope;connection='control:gap-mapping:'+self.scope
        if record.channel!=channel and record.receipt.connection_id!=connection:return
        from .gapmap import SCHEMA,RULE,mapping_payload
        r=record.receipt
        if ((r.capture_id,r.connection_id,record.channel,record.kind,record.native_product,record.instrument,record.native_fields_json)!=
                (self.capture_id,connection,channel,'coverage_event','control',None,b'{}') or
                (record.venue,record.environment)!=self.journal.identity or
                r.receive_seq!=self._mapping_positions.get(r.stream_key,-1)+1 or len(record.body)>65536):
            raise ValueError('native mapping envelope/order differs')
        value=strict_json(record.body)
        if (not isinstance(value,dict) or value.get('schema')!=SCHEMA or value.get('rule')!=RULE or
                value.get('scope')!=self.scope or value.get('recovery_cutoff_id') is not None):
            raise ValueError('native mapping schema/scope differs')
        sha256(value['request_id'],'native mapping request');sha256(value['plan_sha256'],'native mapping plan')
        expected=mapping_payload(value,record.record_id)
        rows=value['raw_dependencies'];refs=[]
        if type(rows) is not list or not rows:raise ValueError('native mapping dependency list required')
        for row in rows:
            if type(row) is not dict or set(row)!={'record_id','raw_sha256'}:raise ValueError('native mapping dependency shape')
            sha256(row['record_id'],'mapped raw identity');sha256(row['raw_sha256'],'mapped raw hash')
            prior=self._evidence.get(row['record_id'])
            if prior is None or prior.capture_id!=self.capture_id or prior.raw_sha256!=row['raw_sha256']:
                raise ValueError('native mapping raw dependency missing/conflicting')
            refs.append(row['record_id'])
        proof=value['native_proof_record_id']
        if len(set(refs))!=len(refs) or expected.evidence_record_id not in refs or (proof is not None and proof not in refs):
            raise ValueError('native mapping proof/discovery dependency missing')
        self._mapping_bytes+=len(record.body)
        if self._mapping_bytes>self._max_mapping_bytes:raise ValueError('native mapping projection byte bound')
        self._pending_mappings.add(record.record_id)
        self._mappings[record.record_id]=value;self._mapping_refs[record.record_id]=tuple(refs)
        self._mapping_positions[r.stream_key]=r.receive_seq

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
        if isinstance(entry.payload,GapCorrection):self._pending_mappings.discard(entry.payload.proof_record_id)
        self._positions[r.stream_key]=r.receive_seq
        self._control_ids[entry.sequence]=record.record_id

    def refresh(self, *, allow_pending_mappings=False):
        if self.poisoned:raise ValueError('ledger must be reopened after failure')
        # Revalidate the entire supplied prefix, including its order and evidence
        # availability. Never certify a cached pin after a dependency disappears.
        self._projection=ValidityLedger(self.ledger_id,**self._projection_limits)
        self._evidence,self._positions,self._control_ids={},{},{}
        self._mappings,self._mapping_refs,self._mapping_positions={},{},{}
        self._mapping_bytes=0;self._pending_mappings=set()
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
                self._restore_mapping(record)
                self._restore_control(record)
                self._evidence[evidence.record_id]=evidence
            if self._pending_mappings and not allow_pending_mappings:
                raise ValueError('unresolved native mapping audit; complete exact correction before certification')
            if self._known_pin is not None:
                self._projection.verify_pin(self._known_pin)
            if self._projection.entry_count:
                self._known_pin=self._projection.pin()
            self._scan_records,self._scan_bytes=count,size
        except BaseException:
            self.poisoned=True
            raise

    def _append(self,payload,*,utc_ns,monotonic_ns,clock_epoch,_completing_mappings=False):
        self.refresh(allow_pending_mappings=isinstance(payload,GapCorrection))
        if self._pending_mappings and (not isinstance(payload,GapCorrection) or payload.proof_record_id not in self._pending_mappings):
            raise ValueError('pending native mapping requires its exact correction')
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
        if isinstance(payload,GapCorrection):self._pending_mappings.discard(payload.proof_record_id)
        self._evidence[evidence.record_id]=evidence
        self._positions[stream]=receipt.receive_seq
        self._control_ids[entry.sequence]=record.record_id
        self._scan_records+=1;self._scan_bytes+=len(record.to_bytes())
        self._known_pin=self._projection.pin()
        self.poisoned=False
        if self._pending_mappings:
            if _completing_mappings:return None
            raise ValueError('other native mapping audits remain unresolved')
        return self._projection.pin()

    def initialize(self,*,utc_ns,monotonic_ns,clock_epoch):
        self.refresh()
        if self._projection.entry_count:return self._projection.pin()
        return self._append(LedgerStart(self.scope),utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)

    def append(self,payload,*,utc_ns,monotonic_ns,clock_epoch):
        if not isinstance(payload,(GapCorrection,CloseGap)):
            raise ValueError('explicit gap/recovery payload required')
        return self._append(payload,utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)

    def recovery_state(self):
        """Source chain tip and exact pending negatives; not a validity certificate.

        Startup may carry this tip solely to assert complete ledger continuity.
        Public pin/assessment APIs still refuse until every audit is corrected.
        """
        from .gapmap import mapping_payload
        self.refresh(allow_pending_mappings=True)
        pending=tuple(mapping_payload(value,identity) for identity,value in self._mappings.items()
                      if identity in self._pending_mappings)
        pin=self._projection.pin() if self._projection.entry_count else None
        if pending and pin is None:raise ValueError('native mapping audit lacks ledger genesis')
        return pin,pending

    def complete_native_mappings(self,*,utc_ns,monotonic_ns,clock_epoch):
        """Finish only already-durable negative audits, in original record order.

        No market replay/positive recovery is inferred. A lost append ack requires
        reopening; restoration removes completed audits before the next attempt.
        """
        _,pending=self.recovery_state()
        for payload in pending:
            self._append(payload,utc_ns=utc_ns,monotonic_ns=monotonic_ns,
                         clock_epoch=clock_epoch,_completing_mappings=True)
        return self.pin(allow_empty=True)

    def pin(self, *, allow_empty=False):
        self.refresh()
        if allow_empty and not self._projection.entry_count:return None
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
        pending=list(identities)
        while pending:
            for dependency in self._mapping_refs.get(pending.pop(),()):
                if dependency not in identities:identities.add(dependency);pending.append(dependency)
        return tuple(self._evidence[identity] for identity in sorted(identities))
