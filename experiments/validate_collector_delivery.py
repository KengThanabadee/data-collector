"""Bounded local validation of production delivery code; no timers or pings.

Run from repo root: .venv/bin/python -m experiments.validate_collector_delivery
Uses a fresh separate data root and at most three 15-second acquisition chunks.
"""
import asyncio
from dataclasses import replace
import json
import time
from option_collector.archive import verify
from option_collector.common import mkdir,process_lock,write_json
from option_collector.config import bind_root,load,retain_source
from option_collector.delivery import chunk
from option_collector.transport import Client


async def main():
    config=load('config/local.toml')
    if config.require_ntp:raise ValueError('This validation is for local development only')
    config=replace(config,data_root=config.data_root.parent/'validation-delivery')
    if config.data_root.exists():raise ValueError('Validation root already exists; preserve its evidence')
    mkdir(config.data_root)
    start=time.monotonic();completed={}
    with process_lock(config.data_root):
        bind_root(config);retain_source(config)
        async with Client(config) as client:
            for _ in range(3):
                for m in await chunk(config,client,time.monotonic()+15):
                    if m['status']=='complete':completed[m['currency']]=m
                if len(completed)==len(config.currencies):break
        summaries={}
        for currency,m in completed.items():
            manifest,records=verify(config.data_root/m['destination'])
            summaries[currency]=dict(status=m['status'],requests=len(records),records_total=sum(len(json.loads(r['raw_body'])['result']['data']) for r in records if r['ok']),destination=m['destination'],archive_sha256=manifest['archive_sha256'])
        result=dict(validation_only=True,elapsed_seconds=time.monotonic()-start,markets=summaries)
        write_json(config.data_root/'validation.json',result)
        print(json.dumps(result))
        if len(completed)!=len(config.currencies):raise ValueError('Delivery did not finish within bounded validation')


if __name__=='__main__':asyncio.run(main())
