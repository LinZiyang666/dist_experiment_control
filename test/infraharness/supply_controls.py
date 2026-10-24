#!/usr/bin/env python3
"""Adversarial input-custody controls; never substitute for a live receipt."""
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


supply, business = module("release_supply"), module("business_supply")


class SourceCustody(unittest.TestCase):
    # origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / F6
    def test_file_binding_rejects_escape_symlink_and_changed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "source"
            path.write_bytes(b"source custody control")
            row = supply.pin(root, path)
            self.assertEqual(supply.verify(root, row), path)
            for name in ("../source", "/source", "a/../source", "./source", "a\\source"):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    supply.verify(root, dict(row, Path=name))
            alias = root / "alias"
            alias.symlink_to(path)
            with self.assertRaises(ValueError):
                supply.verify(root, dict(row, Path="alias"))
            outside = root / "outside"
            outside.mkdir()
            (outside / "source").write_bytes(path.read_bytes())
            parent = root / "parent"
            parent.symlink_to(outside, target_is_directory=True)
            with self.assertRaises(ValueError):
                supply.verify(root, dict(row, Path="parent/source"))
            path.write_bytes(b"changed source")
            with self.assertRaises(ValueError):
                supply.verify(root, row)
            path.write_bytes(b"")
            with self.assertRaises(ValueError):
                supply.regular(path)

    # origin: S9 internal review INFRA-SUITE-SUPPLIES F4
    def test_copy_hashes_destination_after_source_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "source", root / "target"
            source.write_bytes(b"verified original input")
            record = supply.pin(root, source)
            supply.copy_bound(root, record, target)
            self.assertEqual(target.read_bytes(), source.read_bytes())
            target.unlink()
            copy = supply.shutil.copyfile
            def change_before_copy(incoming, destination):
                source.write_bytes(b"changed after verification")
                return copy(incoming, destination)
            with mock.patch.object(supply.shutil, "copyfile", side_effect=change_before_copy):
                with self.assertRaisesRegex(ValueError, "changed during staging"):
                    supply.copy_bound(root, record, target)

    # origin: S9 internal review INFRA-SUITE-SUPPLIES F4
    def test_previous_generation_cannot_be_a_same_source_relabel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def generation(name, source, binary):
                record = {"binaries": [{"component": component, "source_sha256": source, "binary_sha256": binary}
                                       for component in supply.NAMES[:2]]}
                pin = supply.write(root, name + ".json", record)
                return {"Version": "v0.0.0-" + name, "Source": pin}
            old = generation("previous", "ab" * 32, "cd" * 32)
            for source, binary in (("ab" * 32, "ef" * 32), ("ef" * 32, "cd" * 32)):
                present = generation("current" + source[:2], source, binary)
                with self.assertRaisesRegex(ValueError, "distinct executable source and bytes"):
                    supply.distinct(old, present, root)
            good = generation("genuine", "01" * 32, "02" * 32)
            supply.distinct(old, good, root)
            good["Version"] = old["Version"]
            with self.assertRaisesRegex(ValueError, "version-only relabel"):
                supply.distinct(old, good, root)

    # origin: S9 internal review INFRA-SUITE-SUPPLIES F6
    def test_workload_entry_and_every_source_file_remain_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            # These are source-custody fixtures, never executable live evidence.
            (source / "entry.py").write_text("raise SystemExit('source control only')\n")
            (source / "reader.py").write_text("VALUE = 'independent source'\n")
            value = {"Version": 1, "Root": str(source), "Entry": "entry.py",
                     "Files": [supply.pin(source, source / name) for name in ("entry.py", "reader.py")]}
            manifest = root / "source.json"
            manifest.write_bytes(supply.canonical(value))
            entry, copied = business.workload(manifest, root / "copied")
            self.assertEqual(entry.read_bytes(), (source / "entry.py").read_bytes())
            self.assertEqual(copied, value)
            self.assertEqual(supply.regular(entry.parent / "reader.py"), value["Files"][1]["SHA256"])
            for defect in ("missing-entry", "changed-reader", "duplicate-source"):
                original = supply.read_json(manifest)
                if defect == "missing-entry":
                    original["Entry"] = "foreign.py"
                elif defect == "changed-reader":
                    original["Files"][1]["SHA256"] = "aa" * 32
                else:
                    original["Files"].append(original["Files"][0])
                manifest.write_bytes(supply.canonical(original))
                with self.subTest(defect=defect), self.assertRaises(ValueError):
                    business.workload(manifest, root / defect)
                manifest.write_bytes(supply.canonical(value))

    # origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / F6
    def test_duplicate_manifest_keys_cannot_erase_original_bindings(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_bytes(b'{"Version":1,"Version":2}')
            with self.assertRaisesRegex(ValueError, "duplicate supply key"):
                supply.read_json(path)


if __name__ == "__main__":
    unittest.main()
