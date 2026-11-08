"""Bounded v2 capture and independent committed-archive verification."""
import argparse
import asyncio
import json
from pathlib import Path

from option_collector.common import epoch
from option_collector.config import Config as RestConfig

from .app import run, verify_run
from .config import load
from .minute import capture_minutes, read_minute


def main(argv=None):
    parser = argparse.ArgumentParser(prog='data-collector')
    commands = parser.add_subparsers(dest='command',required=True)
    for name in ('config-check','discover','capture'):
        commands.add_parser(name).add_argument('--config',required=True)
    check = commands.add_parser('verify-run')
    check.add_argument('root')
    check.add_argument('--export',help='create a new lossless raw-envelope JSONL file')
    minute_capture = commands.add_parser('minute-capture', help='capture the Deribit REST minute grid')
    minute_capture.add_argument('--data-root', type=Path, required=True)
    minute_capture.add_argument('--environment', choices=('production', 'testnet'), default='production')
    minute_capture.add_argument('--minutes', type=int, help='limit the number of grid slots processed')
    minute_capture.add_argument('--request-window', type=float, default=20, help='REST deadline in seconds after :00 UTC')
    minute_read = commands.add_parser('minute-read', help='verify and read an archived minute')
    minute_read.add_argument('--data-root', type=Path, required=True)
    minute_read.add_argument('--slot', required=True, help='UTC minute boundary, for example 2026-11-08T07:21:00Z')
    args = parser.parse_args(argv)
    try:
        if args.command=='verify-run':
            result = verify_run(args.root,export=args.export)
            code = 0 if result.get('book_replay_equal',result['archive_verified']) else 1
        elif args.command == 'minute-capture':
            config = RestConfig(data_root=args.data_root.expanduser().resolve(), environment=args.environment, attempts=1)
            count = asyncio.run(capture_minutes(config, minutes=args.minutes, request_window=args.request_window))
            result, code = {'minutes_processed': count}, 0
        elif args.command == 'minute-read':
            slot = epoch(args.slot)
            if not slot.is_integer():
                raise ValueError("expected a UTC minute boundary")
            index, records = read_minute(args.data_root.expanduser(), int(slot))
            result, code = {'index': index, 'records': records}, 0
        else:
            config = load(args.config)
            if args.command=='config-check':
                result = dict(config_sha256=config.config_sha256,config=json.loads(config.to_bytes()))
                code = 0
            else:
                result = asyncio.run(run(config,discover_only=args.command=='discover'))
                code = 1 if result['status']=='failed' else 0
        print(json.dumps(result,ensure_ascii=False,indent=2))
        return code
    except KeyboardInterrupt:return 130
    except (ValueError,OSError) as exc:
        print(json.dumps(dict(error=str(exc)),ensure_ascii=False))
        return 1
