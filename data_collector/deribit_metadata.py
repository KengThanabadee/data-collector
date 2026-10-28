"""Discovery-backed dc.metadata/2 records with bounded raw dependency checks.

Economic/native attributes stay separate from acquisition pointers so a later
identical discovery does not manufacture a contract/owner change.
"""
from dataclasses import asdict

from .contracts import InstrumentKey,RawRecord,Receipt,canonical,digest,integer,sha256,strict_json
from .discovery import METHOD,PARAMS,project_instruments
from .metadata import MetadataVersion

SCHEMA='dc.deribit-instrument/1'
PROOF='dc.deribit-metadata-source/1'


def attributes(row):
    return dict(schema=SCHEMA,native=row['attributes']['native'],
                native_decimal_encoding=row['attributes']['native_decimal_encoding'])


def marked(record):
    if record.kind!='metadata_version':return False
    if record.receipt.connection_id=='derived:deribit-metadata':return True
    body=strict_json(record.body)
    return isinstance(body,dict) and isinstance(body.get('attributes'),dict) and body['attributes'].get('schema')==SCHEMA


def _lookup(factory,wanted,*,max_records,max_scan_bytes,max_source_bytes):
    found={};count=size=retained=0
    for position,record in enumerate(factory()):
        if not isinstance(record,RawRecord):raise ValueError('typed metadata proof raw required')
        encoded=record.to_bytes();count+=1;size+=len(encoded)
        if count>max_records or size>max_scan_bytes:raise ValueError('metadata proof scan bound')
        if record.record_id not in wanted:continue
        checksum=digest(encoded)
        if record.record_id in found:
            if found[record.record_id][2]!=checksum:raise ValueError('conflicting metadata proof record')
            continue
        retained+=len(encoded)
        if retained>max_source_bytes:raise ValueError('metadata proof retained byte bound')
        found[record.record_id]=(position,record,checksum)
    if set(found)!=wanted:raise ValueError('metadata discovery dependency missing')
    return found


def _universe(found,request_id,response_id,*,max_instruments):
    a,request,request_hash=found[request_id];b,response,response_hash=found[response_id]
    fields=strict_json(response.native_fields_json);intent=strict_json(request.body)
    endpoint=('https://www.deribit.com/api/v2/' if response.environment=='production' else
              'https://test.deribit.com/api/v2/')+METHOD
    if (response.environment not in {'production','testnet'} or a>=b or
            request.receipt.stream_key!=response.receipt.stream_key or request.receipt.receive_seq+1!=response.receipt.receive_seq or
            request.receipt.monotonic_ns>response.receipt.monotonic_ns or request.kind!='control' or
            request.venue!='deribit' or request.environment!=response.environment or
            request.native_product!='discovery' or request.channel!=METHOD or request.instrument is not None or
            request.provenance!=response.provenance or intent!=dict(method='GET',endpoint=endpoint,params=PARAMS) or
            fields.get('request_record_id')!=request_id or fields.get('request_raw_sha256')!=request_hash or
            fields.get('endpoint')!=endpoint or fields.get('direction')!='inbound'):
        raise ValueError('discovery request/response proof differs')
    request_fields=dict(strict_json(request.native_fields_json));request_fields.pop('collector_ingress_ordinal',None)
    if request_fields!=dict(schema='dc.deribit-discovery/1',direction='outbound',params=PARAMS,dispatched=False):
        raise ValueError('discovery request intent differs')
    start=fields.get('request_start')
    if (not isinstance(start,dict) or set(start)!={'utc_ns','monotonic_ns'} or type(start['monotonic_ns']) is not int or
            not request.receipt.monotonic_ns<=start['monotonic_ns']<=response.receipt.monotonic_ns):
        raise ValueError('discovery acquisition position differs')
    headers=fields.get('headers')
    if not isinstance(headers,list):raise ValueError('discovery headers required')
    for row in headers:
        if not isinstance(row,list) or len(row)!=2 or any(not isinstance(x,str) for x in row):raise ValueError('invalid discovery headers')
        if row[0].lower()=='content-encoding' and row[1].lower()!='identity':raise ValueError('unqualified discovery encoding')
    manifest=project_instruments(response,max_instruments=max_instruments)
    return manifest,strict_json(manifest),response_hash,request_hash


def verify_universe(factory,manifest,*,max_records=200000,max_scan_bytes=256*1024*1024,max_source_bytes=8*1024*1024,max_instruments=10000):
    if type(manifest) is not bytes or len(manifest)>max_source_bytes:raise ValueError('bounded immutable discovery manifest required')
    value=strict_json(manifest)
    if not isinstance(value,dict) or value.get('schema')!='dc.deribit-universe/1':raise ValueError('discovery universe schema differs')
    wanted={value['request_record_id'],value['response_record_id']}
    for identity in wanted:sha256(identity,'discovery proof identity')
    found=_lookup(factory,wanted,max_records=max_records,max_scan_bytes=max_scan_bytes,max_source_bytes=max_source_bytes)
    rebuilt,value,_,_=_universe(found,value['request_record_id'],value['response_record_id'],max_instruments=max_instruments)
    if rebuilt!=manifest or not value['selected']:raise ValueError('universe differs from complete raw discovery')
    return value,found


def metadata_records(value,found,*,receipt,provenance,heads):
    """Build a whole batch after validation; no disk or subscription side effects."""
    response=found[value['response_record_id']][1];request=found[value['request_record_id']][1]
    if ((receipt.capture_id,receipt.process_session_id)!=(response.receipt.capture_id,response.receipt.process_session_id) or
            receipt.monotonic_ns<response.receipt.monotonic_ns or
            (provenance.source_sha256,provenance.config_sha256)!=(response.provenance.source_sha256,response.provenance.config_sha256)):
        raise ValueError('metadata bootstrap source context differs')
    universe_id=digest(canonical(value));records=[]
    request_hash=digest(request.to_bytes());response_hash=digest(response.to_bytes())
    for sequence,row in enumerate(value['selected']):
        key=InstrumentKey(**row['instrument'])
        fields=dict(schema=PROOF,request_record_id=request.record_id,request_raw_sha256=request_hash,
            response_record_id=response.record_id,response_raw_sha256=response_hash,
            universe_sha256=universe_id,row_index=row['attributes']['row_index'])
        r=Receipt(receipt.capture_id,receipt.process_session_id,receipt.connection_id,receipt.receive_seq+sequence,
                  receipt.receive_utc_ns,receipt.monotonic_ns,receipt.clock_epoch)
        records.append(RawRecord(r,key.venue,key.environment,key.native_product,'metadata','metadata_version',
            canonical(dict(schema='dc.metadata/2',effective_at_ns=None,attributes=attributes(row),supersedes=heads.get(key))),
            provenance,key,canonical(fields)))
    return tuple(records)


def verify_metadata_sources(factory,records,*,max_records=200000,max_scan_bytes=256*1024*1024,max_source_bytes=8*1024*1024):
    selected=[r for r in records if marked(r)]
    if not selected:return {}
    wanted=set();proofs={}
    for record in selected:
        fields=strict_json(record.native_fields_json)
        expected={'schema','request_record_id','request_raw_sha256','response_record_id','response_raw_sha256',
                  'universe_sha256','row_index','collector_ingress_ordinal'}
        if set(fields)!=expected or fields['schema']!=PROOF:raise ValueError('metadata discovery proof required')
        for field in ('request_record_id','request_raw_sha256','response_record_id','response_raw_sha256','universe_sha256'):
            sha256(fields[field],field)
        integer(fields['row_index'],'native row index');integer(fields['collector_ingress_ordinal'],'metadata ingress ordinal')
        wanted.update((record.record_id,fields['request_record_id'],fields['response_record_id']));proofs[record.record_id]=fields
    found=_lookup(factory,wanted,max_records=max_records,max_scan_bytes=max_scan_bytes,max_source_bytes=max_source_bytes)
    cache={};references={};heads={};cached_bytes=0
    for record in sorted(selected,key=lambda r:found[r.record_id][0]):
        fields=proofs[record.record_id];request_id=fields['request_record_id'];response_id=fields['response_record_id']
        if response_id not in cache:
            encoded,value,response_hash,request_hash=_universe(found,request_id,response_id,max_instruments=10000)
            cached_bytes+=len(encoded)
            if cached_bytes>max_source_bytes:raise ValueError('metadata universe projection byte bound')
            cache[response_id]=(digest(encoded),value,response_hash,request_hash,
                {row['attributes']['row_index']:row for row in value['selected']})
        universe_id,value,response_hash,request_hash,rows=cache[response_id]
        response=found[response_id][1]
        row=rows.get(fields['row_index'])
        body=strict_json(record.body)
        if (row is None or value['request_record_id']!=request_id or universe_id!=fields['universe_sha256'] or
                request_hash!=fields['request_raw_sha256'] or response_hash!=fields['response_raw_sha256'] or
                found[record.record_id][2]!=digest(record.to_bytes()) or found[response_id][0]>=found[record.record_id][0] or
                record.instrument is None or asdict(record.instrument)!=row['instrument'] or
                record.receipt.capture_id!=response.receipt.capture_id or record.receipt.process_session_id!=response.receipt.process_session_id or
                record.receipt.monotonic_ns<response.receipt.monotonic_ns or
                (record.provenance.source_sha256,record.provenance.config_sha256)!=(response.provenance.source_sha256,response.provenance.config_sha256) or
                body!=dict(schema='dc.metadata/2',effective_at_ns=None,attributes=attributes(row),
                    supersedes=heads.get((record.receipt.stream_key,record.instrument),(None,None))[0])):
            raise ValueError('metadata differs from pinned native discovery')
        previous=heads.get((record.receipt.stream_key,record.instrument),(None,None))[1]
        dependencies={request_id,response_id}
        if previous is not None:dependencies.add(previous)
        references[record.record_id]=tuple(sorted(dependencies))
        version=MetadataVersion(record.instrument,record.receipt,None,canonical(body['attributes']),body['supersedes'])
        heads[(record.receipt.stream_key,record.instrument)]=(version.version_id,record.record_id)
    return references
