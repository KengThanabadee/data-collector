from dataclasses import FrozenInstanceError, replace
from pathlib import Path
import tempfile
import unittest

from data_collector.config import DiscoveryConfig, MAX_CONFIG_BYTES, SCHEMA, load
from data_collector.contracts import strict_json
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

    def test_byte_bound_and_invalid_encoding(self):
        with self.assertRaisesRegex(ValueError, "byte bound"):
            self.read(self.body + "#" * MAX_CONFIG_BYTES)
        self.path.write_bytes(b"\xff")
        with self.assertRaises(ValueError):
            load(self.path)

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
