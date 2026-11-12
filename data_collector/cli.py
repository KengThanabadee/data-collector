"""Bounded v2 capture and independent committed-archive verification."""
import argparse
import asyncio
import json
from pathlib import Path

from .minute_files import epoch
from .minute_config import Config as RestConfig

from .app import run, verify_run
from .config import load
from .minute import capture_minutes, read_minute, read_ticker_samples
from .minute_bybit import BASE_URLS as BYBIT_URLS, DEFAULT_COUNTS, DEFAULT_EXPIRIES
from .minute_binance import BASE_URLS as BINANCE_URLS, DEFAULT_BOT_RESERVE, DEFAULT_IP_LIMIT, DEFAULT_RATE
from .minute_storage import confirm_offsite_copy, prune_minute_day, seal_minute_day, verify_minute_day
from .minute_tickers import WS_URLS, read_deribit_retirements


def main(argv=None):
    parser = argparse.ArgumentParser(prog='data-collector')
    commands = parser.add_subparsers(dest='command',required=True)
    for name in ('config-check','discover','capture'):
        commands.add_parser(name).add_argument('--config',required=True)
    check = commands.add_parser('verify-run')
    check.add_argument('root')
    check.add_argument('--export',help='create a new lossless raw-envelope JSONL file')
    minute_capture = commands.add_parser('minute-capture', help='capture the shared market-data minute grid')
    minute_capture.add_argument('--data-root', type=Path, required=True)
    minute_capture.add_argument('--environment', choices=('production', 'testnet'), default='production')
    minute_capture.add_argument('--deribit-only', action='store_true', help='skip the Bybit and Binance REST profiles')
    minute_capture.add_argument('--no-binance', action='store_true', help='skip the Binance REST profile')
    minute_capture.add_argument('--binance-symbols', help='comma-separated override for the fixed Binance universe')
    minute_capture.add_argument('--binance-ip-limit', type=int, default=DEFAULT_IP_LIMIT)
    minute_capture.add_argument('--binance-bot-reserve', type=int, default=DEFAULT_BOT_RESERVE)
    minute_capture.add_argument('--binance-rate', type=float, default=DEFAULT_RATE)
    minute_capture.add_argument('--bybit-depth-btc', type=int, default=DEFAULT_COUNTS[0])
    minute_capture.add_argument('--bybit-depth-eth', type=int, default=DEFAULT_COUNTS[1])
    minute_capture.add_argument('--bybit-expiries', type=int, default=DEFAULT_EXPIRIES)
    minute_capture.add_argument('--rest-only', action='store_true', help='skip the Deribit ticker sampler')
    minute_capture.add_argument('--minutes', type=int, help='limit the number of grid slots processed')
    minute_capture.add_argument('--request-window', type=float, default=20, help='REST deadline in seconds after :00 UTC')
    minute_read = commands.add_parser('minute-read', help='verify and read an archived minute')
    minute_read.add_argument('--data-root', type=Path, required=True)
    minute_read.add_argument('--slot', required=True, help='UTC minute boundary, for example 2026-11-08T07:21:00Z')
    archive_commands = {
        'minute-seal': 'seal a closed UTC day from hourly journals',
        'minute-verify-day': 'verify a sealed UTC day and its manifest',
        'minute-confirm-offsite': 'record the checksum of an offsite archive copy',
        'minute-prune': 'remove verified redundant minute journals and WAL',
    }
    for name, help_text in archive_commands.items():
        command = commands.add_parser(name, help=help_text)
        command.add_argument('--data-root', type=Path, required=True)
        command.add_argument('--day', required=True, help='UTC date in YYYY-MM-DD format')
        if name == 'minute-confirm-offsite':
            command.add_argument('--sha256', required=True, help='SHA-256 read from the offsite copy')
    args = parser.parse_args(argv)
    try:
        if args.command=='verify-run':
            result = verify_run(args.root,export=args.export)
            code = 0 if result.get('book_replay_equal',result['archive_verified']) else 1
        elif args.command == 'minute-capture':
            config = RestConfig(data_root=args.data_root.expanduser().resolve(), environment=args.environment, attempts=1)
            ticker_url = None if args.rest_only else WS_URLS[args.environment]
            bybit_url = None if args.deribit_only else BYBIT_URLS[args.environment]
            binance_url = None if args.deribit_only or args.no_binance else BINANCE_URLS[args.environment]
            symbols = None if args.binance_symbols is None else [value.strip() for value in args.binance_symbols.split(',')]
            count = asyncio.run(capture_minutes(
                config, minutes=args.minutes, request_window=args.request_window, ticker_ws_url=ticker_url,
                bybit_base_url=bybit_url, bybit_depth=(args.bybit_depth_btc, args.bybit_depth_eth),
                bybit_expiries=args.bybit_expiries, binance_base_url=binance_url, binance_symbols=symbols,
                binance_ip_limit=args.binance_ip_limit, binance_bot_reserve=args.binance_bot_reserve,
                binance_rate=args.binance_rate,
            ))
            result, code = {'minutes_processed': count}, 0
        elif args.command == 'minute-read':
            slot = epoch(args.slot)
            if not slot.is_integer():
                raise ValueError("expected a UTC minute boundary")
            index, records = read_minute(args.data_root.expanduser(), int(slot))
            result, code = {'index': index, 'records': records}, 0
            if 'deribit_tickers_sha256' in index:
                result['deribit_tickers'] = read_ticker_samples(args.data_root.expanduser(), int(slot))
            retirements = read_deribit_retirements(args.data_root.expanduser(), int(slot))
            if retirements:
                result['deribit_retirements'] = retirements
        elif args.command in archive_commands:
            root = args.data_root.expanduser().resolve()
            if args.command == 'minute-seal':
                result = seal_minute_day(root, args.day)
            elif args.command == 'minute-verify-day':
                result = verify_minute_day(root, args.day)
            elif args.command == 'minute-confirm-offsite':
                result = confirm_offsite_copy(root, args.day, args.sha256)
            else:
                result = {'removed_files': prune_minute_day(root, args.day)}
            code = 0
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
