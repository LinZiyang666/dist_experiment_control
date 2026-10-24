#!/usr/bin/env -S python3 -I -B
"""Product CLI bridge; Go's P18 Driver remains the only command authority.

P19 supplies call_driver(method, payload), connected to the Go fixture adapter.
Any process used by that connection must use P05's shared supervisor and join
its waiter. This module never executes argv found in evidence, repairs product
state, retries a failed mutation, or substitutes an old CLI for a missing one.
Missing S8/S9 bindings retain NOT_RUN; this bridge cannot certify completion.

origin: hot-update S10 plan §5.1 / §10.2 P18
"""

import copy
import hashlib
import json


class PendingProductCLI(RuntimeError):
    """A frozen upstream binding or its native observation adapter is absent."""


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate product CLI response member")
        value[key] = item
    return value


def _decode(raw):
    if type(raw) is not bytes:
        raise ValueError("product CLI response must retain original bytes")
    return json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_object,
                      parse_constant=lambda _: _invalid_number())


def _invalid_number():
    raise ValueError("non-finite product CLI response number")


class ProductCLIDriver:
    """Forward typed requests once to the frozen Go Driver port.

    call_driver returns the original JSON bytes of a P00 View or raises its
    native command error. Error objects and bytes are not rewritten. The Go
    driver retains raw stdout/stderr, native rc and durable dispatch intent.
    Process timeout/cancellation and the sole waiter remain with that driver.
    """

    def __init__(self, call_driver=None):
        self._call = call_driver
        self._sent = set()
        self._queries = set()
        self.raw = []

    def stage(self, step):
        return self._invoke("stage", step, unsafe=True)

    def apply(self, step):
        return self._invoke("apply", step, unsafe=True)

    def query(self, key, tx_id, nonce):
        if not tx_id or not nonce:
            raise ValueError("query requires original transaction and fresh nonce")
        token = json.dumps([key, nonce], sort_keys=True, allow_nan=False)
        if token in self._queries:
            raise ValueError("query nonce was already used")
        self._queries.add(token)
        return self._invoke("query", {"key": key, "tx_id": tx_id, "nonce": nonce}, unsafe=False)

    def _invoke(self, method, payload, *, unsafe):
        if self._call is None:
            raise PendingProductCLI("NOT_RUN pending P19 Go Driver bridge and S8/S9 CLI bindings")
        if type(payload) is not dict:
            raise ValueError("typed product CLI request required")
        # Claim before dispatch and keep the claim when the reply is lost. Go
        # owns the durable claim across driver/fixture reconstruction as well.
        if unsafe:
            if not payload.get("id") or type(payload.get("target")) is not dict:
                raise ValueError("sealed step ID and target required")
            token = json.dumps([method, payload["id"], payload["target"]], sort_keys=True, allow_nan=False)
            if token in self._sent:
                raise ValueError("unsafe invocation already sent; query the original transaction")
            self._sent.add(token)
        raw = self._call(method, copy.deepcopy(payload))
        # Retain even malformed bytes: decoder failure must not erase evidence.
        self.raw.append(raw)
        value = _decode(raw)
        if type(value) is not dict or set(value) != {
                "stamp", "historical", "current", "phase", "outcome", "code", "detail",
                "deferred", "control", "resources", "released", "raw"}:
            raise ValueError("response is not a complete P00 View")
        return value

    def raw_digest(self, index):
        """Digest bytes, never a reserialized or filtered product response."""
        return hashlib.sha256(self.raw[index]).hexdigest()


def selftest():
    """Hermetic protocol checks; no product, supervisor or deployment launches."""
    import unittest

    class ProductCLIBridgeTest(unittest.TestCase):
        def test_lost_reply_never_repeats_unsafe_call(self):
            calls = []

            def lost(method, payload):
                calls.append((method, payload))
                raise OSError("lost reply")

            driver = ProductCLIDriver(lost)
            step = {"id": "R1-agent", "target": {"unit": "agent-a"}}
            with self.assertRaisesRegex(OSError, "lost reply"):
                driver.apply(step)
            step["direction"] = "reverse"
            with self.assertRaisesRegex(ValueError, "already sent"):
                driver.apply(step)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][1], {"id": "R1-agent", "target": {"unit": "agent-a"}})

        def test_absent_backend_is_pending(self):
            with self.assertRaisesRegex(PendingProductCLI, "NOT_RUN pending"):
                ProductCLIDriver().stage({"id": "R1-agent", "target": {}})

        def test_raw_rejection_preserves_bytes_and_nonce(self):
            raw = b'{"phase":"completed","phase":"deferred"}'
            calls = []

            def reply(method, payload):
                calls.append(method)
                return raw

            driver = ProductCLIDriver(reply)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                driver.query({"unit": "agent-a"}, "tx", "nonce")
            with self.assertRaisesRegex(ValueError, "already used"):
                driver.query({"unit": "agent-a"}, "tx", "nonce")
            self.assertEqual(driver.raw, [raw])
            self.assertEqual(driver.raw_digest(0), hashlib.sha256(raw).hexdigest())
            self.assertEqual(calls, ["query"])

    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ProductCLIBridgeTest))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    import sys
    if sys.argv[1:] != ["--selftest"]:
        raise SystemExit("fullstack_cli requires the P19 Go bridge; only --selftest is standalone")
    raise SystemExit(selftest())
