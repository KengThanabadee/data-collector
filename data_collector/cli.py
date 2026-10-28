"""Bounded v2 capture and independent committed-archive verification."""
import argparse
import asyncio
import json

from .app import run, verify_run
from .config import load


def main(argv=None):
    parser = argparse.ArgumentParser(prog='data-collector')
    commands = parser.add_subparsers(dest='command',required=True)
    for name in ('config-check','discover','capture'):
        commands.add_parser(name).add_argument('--config',required=True)
    check = commands.add_parser('verify-run')
    check.add_argument('root')
    check.add_argument('--export',help='create a new lossless raw-envelope JSONL file')
    args = parser.parse_args(argv)
    try:
        if args.command=='verify-run':
            result = verify_run(args.root,export=args.export)
            code = 0 if result['book_replay_equal'] else 1
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
