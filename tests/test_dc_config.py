from dataclasses import FrozenInstanceError, replace
from pathlib import Path
import tempfile
import unittest

from data_collector.config import DiscoveryConfig, MAX_CONFIG_BYTES, SCHEMA, load
from data_collector.contracts import strict_json
from data_collector.bybit_endpoints import public_origins
from option_collector.config import load as load_v1


class DiscoveryConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / "collector.toml"
        self.body = ('[data_collector]\nschema = "dc.config/2"\n'
                     'environment = "testnet"\ndata_root = "archive"\n')

    def read(self, body=None):
        self.path.write_text(self.body if body is None else body)
        return load(self.path)

    def test_effective_defaults_relative_paths_and_no_side_effects(self):
        config = self.read()
        self.assertEqual(config.data_root, self.root / "archive")
        self.assertEqual(config.environment, "testnet")
        self.assertEqual(config.max_instruments, 10000)
        self.assertEqual(strict_json(config.to_bytes())["schema"], SCHEMA)
        self.assertFalse(config.data_root.exists())
        with self.assertRaises(FrozenInstanceError):
            config.environment = "production"

    def test_bybit_public_hosts_are_configurable_pinned_and_environment_aware(self):
        default=self.read()
        self.assertEqual(public_origins(default),('https://api-testnet.bybit.com','wss://stream-testnet.bybit.com/v5/public/option'))
        production=replace(default,environment='production')
        self.assertEqual(public_origins(production),('https://api.bybit.com','wss://stream.bybit.com/v5/public/option'))
        alternate=self.read(self.body+'bybit_rest_host="api.bytick.com"\nbybit_ws_host="stream.bytick.com"\n')
        self.assertEqual(public_origins(alternate),('https://api.bytick.com','wss://stream.bytick.com/v5/public/option'))
        self.assertEqual(strict_json(alternate.to_bytes())['bybit_rest_host'],'api.bytick.com')
        self.assertNotEqual(default.config_sha256,alternate.config_sha256)
        for host in ('https://api.bytick.com','user@api.bytick.com','api.bytick.com/path','api.bytick.com:443','a..com','-a.com','a.com?x=1','a.com\n',123):
            for field in ('bybit_rest_host','bybit_ws_host'):
                with self.subTest(field=field,host=host),self.assertRaises(ValueError):replace(default,**{field:host})

    def test_effective_hash_ignores_formatting_but_pins_every_setting(self):
        original = self.read()
        reformatted = self.read("# no secret comments are retained\n" + self.body +
                                'venue = "deribit"\ntimeout_seconds = 30\n')
        self.assertEqual(original.to_bytes(), reformatted.to_bytes())
        self.assertEqual(original.config_sha256, reformatted.config_sha256)
        for changes in ({"environment": "production"}, {"data_root": self.root / "other"},
                        {"max_response_bytes": 1000}, {"max_instruments": 20},
                        {"timeout_seconds": 15}, {"warmup_timeout_seconds": 90},
                        {"chronyc": "/usr/bin/chronyc"}):
            with self.subTest(changes=changes):
                self.assertNotEqual(original.config_sha256,
                                    replace(original, **changes).config_sha256)

    def test_ambiguous_version_environment_and_unknown_fields_rejected(self):
        for body in ('[collector]\nenvironment = "testnet"\n',
                     self.body + '[extra]\nvalue = 1\n',
                     self.body + 'token = "never archive"\n',
                     self.body.replace('schema = "dc.config/2"\n', ''),
                     self.body.replace('environment = "testnet"\n', ''),
                     self.body.replace('data_root = "archive"\n', ''),
                     self.body.replace('dc.config/2', 'dc.config/3'),
                     self.body.replace('"testnet"', '"mainnet"'),
                     self.body + 'venue = "bybit"\n',
                     'data_collector = "not a table"\n',
                     self.body + 'environment = "production"\n'):
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.read(body)

    def test_types_and_resource_bounds_rejected_without_coercion(self):
        for field, maximum in (("max_response_bytes", 16 * 1024 * 1024),
                               ("max_instruments", 10000), ("timeout_seconds", 60),
                               ("warmup_timeout_seconds", 600)):
            for value in ("true", "1.0", '"1"', "0", "-1", str(maximum + 1)):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.read(self.body + f"{field} = {value}\n")
        for value in ('true', '1', '[]', '""'):
            with self.subTest(root=value), self.assertRaises(ValueError):
                self.read(self.body.replace('"archive"', value))
        for value in ('true', '[]', '""', '"-c"'):
            with self.subTest(chronyc=value), self.assertRaises(ValueError):
                self.read(self.body + f"chronyc = {value}\n")

    def test_all_option_plan_requires_raw_mode_and_exclusive_selection(self):
        config=self.read(self.body+'all_options=true\n')
        self.assertTrue(config.all_options)
        self.assertEqual(config.symbols,())
        self.assertEqual(config.subscription_batch_size,500)
        for extra in ('all_options=1\n','all_options=true\nsymbols=["BTC-X-C"]\n',
                'all_options=true\ncapture_mode="snapshots"\nduration_seconds=60\nmax_frames=4\n',
                'subscription_batch_size=501\n','subscription_batch_size=0\n',
                'bootstrap_timeout_seconds=0\n'):
            with self.subTest(extra=extra),self.assertRaises(ValueError):self.read(self.body+extra)

    def test_byte_bound_and_invalid_encoding(self):
        with self.assertRaisesRegex(ValueError, "byte bound"):
            self.read(self.body + "#" * MAX_CONFIG_BYTES)
        self.path.write_bytes(b"\xff")
        with self.assertRaises(ValueError):
            load(self.path)

    def test_raw_segment_size_is_configurable_and_pinned(self):
        default=self.read()
        self.assertEqual(default.raw_segment_bytes,8*1024*1024)
        custom=self.read(self.body+'raw_segment_bytes=4194304\n')
        self.assertEqual(custom.raw_segment_bytes,4*1024*1024)
        self.assertNotEqual(custom.config_sha256,default.config_sha256)
        for value in ('true','1048575','67108865'):
            with self.subTest(value=value),self.assertRaises(ValueError):
                self.read(self.body+f'raw_segment_bytes={value}\n')

    def test_all_futures_is_an_exclusive_raw_family_selection(self):
        config=self.read(self.body+'all_futures=true\n')
        self.assertTrue(config.all_futures)
        for extra in ('all_futures=1\n','all_futures=true\nall_options=true\n',
                'all_futures=true\nsymbols=["BTC-PERPETUAL"]\n',
                'all_futures=true\ncapture_mode="snapshots"\nduration_seconds=60\nmax_frames=4\n'):
            with self.subTest(extra=extra),self.assertRaises(ValueError):self.read(self.body+extra)

    def test_resolved_path_alias_and_absolute_root(self):
        archive = self.root / "existing"
        archive.mkdir()
        (self.root / "alias").symlink_to(archive, target_is_directory=True)
        first = self.read(self.body.replace('"archive"', '"alias/../existing"'))
        second = self.read(self.body.replace('"archive"', f'"{archive}"'))
        self.assertEqual(first.to_bytes(), second.to_bytes())

    def test_cross_version_parsers_and_checked_in_example(self):
        repository = Path(__file__).resolve().parents[1]
        example = repository / "config/v2-discovery.toml"
        config = load(example)
        self.assertEqual(config.environment, "testnet")
        self.assertEqual(config.data_root, repository / "data/v2/testnet")
        with self.assertRaises(ValueError):
            load_v1(example)
        for name in ("local.toml", "vps.toml"):
            self.assertIsNotNone(load_v1(repository / "config" / name))
            with self.assertRaises(ValueError):
                load(repository / "config" / name)
        with self.assertRaises(ValueError):
            DiscoveryConfig(Path("relative"), "testnet")
