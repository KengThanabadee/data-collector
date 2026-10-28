import asyncio
from dataclasses import replace
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from data_collector.contracts import Receipt,RawRecord,StreamKey,canonical,strict_json
from data_collector.deribit_metadata import marked,verify_metadata_sources,verify_universe
from data_collector.lane import DurableMetadataAck,PENDING_CLOCK_EPOCH,DurabilityUnknown
from data_collector.owners import OwnerPlans,new_owner_epoch
from data_collector.replay import BookBinding,SnapshotPlan,ReplayLimits
from data_collector.validity import FeedTarget
from data_collector.snapshots import publish_snapshot
from data_collector.exports import export_snapshots
from tests import test_dc_discovery as base

C=60_000_000_000


class MetadataTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.DiscoveryTests.setUp
    observe=base.DiscoveryTests.observe
    start=base.DiscoveryTests.start
    clock_request=base.DiscoveryTests.clock_request
    clock_policy=base.DiscoveryTests.clock_policy
    record=base.DiscoveryTests.record
    received=base.DiscoveryTests.received
    records=base.DiscoveryTests.records
    discovery=base.DiscoveryTests.discovery
    advance=base.DiscoveryTests.advance
    finish=base.DiscoveryTests.finish

    def lane(self,**kwargs):return base.DiscoveryTests.lane(self,cutoffs=True,snapshot_limits=getattr(self,'metadata_limits',None),**kwargs)
    async def batch(self,lane,manifest):
        utc,mono,_=self.observe()
        ack=await lane.submit_deribit_metadata(manifest,utc_ns=utc,monotonic_ns=mono)
        self.assertIsInstance(ack,DurableMetadataAck);self.assertTrue(ack.timing_valid)
        return ack
    def plan(self,version):
        key=version.instrument
        return SnapshotPlan((BookBinding(FeedTarget(key,'deribit_full',new_owner_epoch()),
            StreamKey('capture','process','books'),version.receipt.stream_key,version.version_id),),'b'*64)
    async def declare(self,lane,plan):
        utc,mono,_=self.observe();return await lane.submit_owner_plan(plan,utc_ns=utc,monotonic_ns=mono)

    async def test_durable_metadata_matches_raw_and_unknown_effective_time(self):
        lane,discovery=await self.discovery()
        manifest=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        ack=await self.batch(lane,manifest);version=ack.versions[0]
        self.assertIsNone(version.effective_at_ns)
        self.assertEqual(version.attributes['native']['settlement_currency'],'USDC')
        self.assertNotIn('source_record_id',version.attributes)
        plan=self.plan(version);await self.declare(lane,plan)
        await self.finish(lane);raw=self.records(lane);derived=[r for r in raw if marked(r)]
        dependencies=verify_metadata_sources(lambda:iter(raw),derived)
        self.assertEqual(set(dependencies[derived[0].record_id]),
            {strict_json(manifest)['request_record_id'],strict_json(manifest)['response_record_id']})
        rebuilt,_=verify_universe(lambda:iter(raw),manifest)
        self.assertEqual(rebuilt,strict_json(manifest))
        self.assertEqual(derived[0].receipt,version.receipt)

    async def test_identical_new_discovery_preserves_native_attributes_and_owner_epoch(self):
        lane,discovery=await self.discovery()
        first=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        before=(await self.batch(lane,first)).versions[0];plan=self.plan(before);await self.declare(lane,plan)
        self.advance(discovery)
        later=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        after=(await self.batch(lane,later)).versions[0]
        self.assertNotEqual(before.version_id,after.version_id)
        self.assertEqual(before.attributes_json,after.attributes_json);self.assertEqual(after.supersedes,before.version_id)
        revised=replace(plan,bindings=(replace(plan.bindings[0],metadata_version_id=after.version_id),))
        result=await self.declare(lane,revised)
        self.assertEqual(result.declaration.plan.bindings[0].target.owner_epoch,plan.bindings[0].target.owner_epoch)
        await self.finish(lane);raw=self.records(lane)
        verify_metadata_sources(lambda:iter(raw),[r for r in raw if marked(r)])

    async def test_missing_mutated_future_or_stripped_discovery_proof_fails_closed(self):
        lane,discovery=await self.discovery()
        manifest=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        await self.batch(lane,manifest);await self.finish(lane);raw=self.records(lane)
        metadata=next(r for r in raw if marked(r));fields=strict_json(metadata.native_fields_json)
        response=next(r for r in raw if r.record_id==fields['response_record_id'])
        request=next(r for r in raw if r.record_id==fields['request_record_id'])
        cases=[]
        cases.append(([r for r in raw if r.record_id!=request.record_id],metadata))
        cases.append(([replace(r,body=b'{}') if r.record_id==request.record_id else r for r in raw],metadata))
        cases.append(([r for r in raw if r.record_id!=response.record_id]+[response],metadata))
        changed=replace(metadata,native_fields_json=b'{}')
        cases.append(([changed if r.record_id==metadata.record_id else r for r in raw],changed))
        stripped=strict_json(metadata.body);stripped['attributes'].pop('schema')
        changed=replace(metadata,body=canonical(stripped),native_fields_json=b'{}')
        legacy_like=[changed if r.record_id==metadata.record_id else r for r in raw
            if r.record_id not in {request.record_id,response.record_id}]
        cases.append((legacy_like,changed))
        empty=SimpleNamespace(identity=('deribit','production'),encoded_records=lambda **kw:iter(()))
        owners=OwnerPlans(empty,capture_scope='capture',capture_id='capture',process_session_id='process',
            provenance=lane.provenance,prior_records=lambda:iter(legacy_like))
        with self.assertRaisesRegex(ValueError,'proof required'):owners.refresh(for_declaration=True)
        body=strict_json(metadata.body);body['attributes']['native']['tick_size']='1'
        changed=replace(metadata,body=canonical(body))
        cases.append(([changed if r.record_id==metadata.record_id else r for r in raw],changed))
        for source,record in cases:
            with self.subTest(source=len(source)),self.assertRaises(ValueError):
                verify_metadata_sources(lambda:iter(source),[record])

    async def test_forged_batch_is_rejected_before_any_metadata_write(self):
        lane,discovery=await self.discovery()
        manifest=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        body=strict_json(manifest);body['selected'][0]['attributes']['native']['tick_size']='999'
        utc,mono,_=self.observe()
        with self.assertRaises(DurabilityUnknown):
            await lane.submit_deribit_metadata(canonical(body),utc_ns=utc,monotonic_ns=mono)
        await lane.test_task
        self.assertFalse(lane.health()['accepting'])
        # The writer failed before appending a metadata record; inspect unsealed
        # source archives through explicit recovery in the existing fault tests.
        self.assertEqual(lane.ingress.positions.get(StreamKey('capture','process','derived:deribit-metadata')),None)

    async def test_combined_proof_budget_is_checked_before_metadata_append(self):
        self.metadata_limits=ReplayLimits(max_metadata_bytes=4000)
        lane,discovery=await self.discovery()
        manifest=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        utc,mono,_=self.observe()
        with self.assertRaises(DurabilityUnknown):
            await lane.submit_deribit_metadata(manifest,utc_ns=utc,monotonic_ns=mono)
        await lane.test_task
        source=StreamKey('capture','process','derived:deribit-metadata')
        self.assertNotIn(source,lane.ingress.positions)
        self.assertFalse(lane.health()['accepting'])

    async def test_cancelled_client_future_does_not_cancel_metadata_write(self):
        lane,discovery=await self.discovery()
        manifest=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def blocked(record):
            if marked(record):
                entered.set()
                if not release.wait(3):raise OSError('test metadata timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=blocked):
            utc,mono,_=self.observe();future=lane.submit_deribit_metadata(manifest,utc_ns=utc,monotonic_ns=mono)
            self.assertTrue(await asyncio.to_thread(entered.wait,2));future.cancel();release.set()
            async def done():
                while lane.queue.health()['bytes']:await asyncio.sleep(0)
            await asyncio.wait_for(done(),5)
        self.assertTrue(lane.health()['accepting']);await self.finish(lane)
        raw=self.records(lane);metadata=[r for r in raw if marked(r)]
        self.assertEqual(len(metadata),1);verify_metadata_sources(lambda:iter(raw),metadata)

    async def test_metadata_namespace_cannot_be_written_by_raw_adapter(self):
        lane,discovery=await self.discovery()
        record=replace(self.received(lane),receipt=replace(self.received(lane).receipt,connection_id='derived:deribit-metadata'))
        with self.assertRaisesRegex(ValueError,'namespace'):lane.submit_received(record)

    async def test_gap_audit_keeps_discovery_dependencies(self):
        lane,discovery=await self.discovery()
        manifest=await discovery.fetch(base.HTTP([base.Response(base.response_body())]))
        version=(await self.batch(lane,manifest)).versions[0];plan=self.plan(version);await self.declare(lane,plan)
        binding=plan.bindings[0]
        async def native(seq,gap=False):
            utc,mono,_=self.observe()
            data=dict(type='change' if gap else 'snapshot',timestamp=1,instrument_name=version.instrument.native_symbol,
                change_id=21 if gap else 20,bids=[['change' if gap else 'new',100,2]],asks=[])
            if gap:data['prev_change_id']=999
            return await lane.submit_received(RawRecord(Receipt('capture','process','books',seq,utc,mono,PENDING_CLOCK_EPOCH),
                'deribit','production','option',binding.native_channel,'ws_message',
                canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=binding.native_channel,data=data))),lane.provenance))
        utc,mono,_=self.observe();cutoff=(utc//C+1)*C
        await lane.submit_cutoff_allocation(cutoff,[binding.ws_source,binding.metadata_source],plan.dependencies,
            utc_ns=utc,monotonic_ns=mono,boot_id='boot')
        await native(0);self.mono[0]+=cutoff-utc
        utc,mono,_=self.observe()
        closed=await lane.submit_cutoff_close(0,[binding.ws_source,binding.metadata_source],utc_ns=utc,monotonic_ns=mono,boot_id='boot')
        inputs=await lane.submit_snapshot_inputs(closed.record_id,utc_ns=utc,monotonic_ns=mono)
        snapshot=publish_snapshot(self.root/'derived-snapshots',[item.path for item in inputs.segments],closed.record_id,plan)
        self.assertEqual(strict_json(snapshot['payload'])['books'][0]['validity_as_published'],'valid')
        self.mono[0]+=1;ack=await native(1,True);utc,mono,_=self.observe()
        result=await lane.submit_native_gap(plan,0,ack.record_id,utc_ns=utc,monotonic_ns=mono)
        self.assertIsNotNone(result.mapping);await self.finish(lane)
        audit=next(strict_json(r.body) for r in self.records(lane) if r.channel=='gap_mapping:capture')
        dependencies={row['record_id'] for row in audit['raw_dependencies']}
        body=strict_json(manifest)
        self.assertTrue({body['request_record_id'],body['response_record_id']}<=dependencies)
        exported=export_snapshots(self.root/'derived-snapshots',self.root/'derived-export',[snapshot['snapshot_id']],lane.segments)
        self.assertEqual(exported['descriptor']['population']['invalid'],1)
