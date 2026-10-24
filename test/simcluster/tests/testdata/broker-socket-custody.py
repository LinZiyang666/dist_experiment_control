"""Original socket and collector exit controls for the real Transport owner."""
import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
import time
from types import SimpleNamespace


# origin: S10 internal review RUNALL-A F3 / hot-update S6 plan §12 P29-P30.
def exercise_socket_process(test, module, here):
    # Natural cleanup must join native0. A collector that outlives its ACK
    # remains an error even after Transport successfully terminates and joins.
    for mode in ("natural", "wedged"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transport = module.Transport(SimpleNamespace(here=here, instance="hermetic", sim="/unused"))
            transport.prefix = [sys.executable, "-I", "-B", "-c"]
            script = ('import pathlib,socket,sys,time; '
                      'listener=socket.socket();listener.bind(("127.0.0.1",0));listener.listen();'
                      'pathlib.Path(sys.argv[1]).write_text(str(listener.getsockname()[1]));'
                      'conn,_=listener.accept();conn.sendall(conn.recv(16));'
                      'assert conn.recv(1)==b"";'
                      'time.sleep(60) if sys.argv[3]=="wedged" else None;'
                      'conn.close();listener.close();pathlib.Path(sys.argv[2]).write_text("closed")')
            child = transport.start(root / "out", root / "err", script, str(root / "port"),
                                    str(root / "closed"), mode)
            closed_by_owner = False
            try:
                deadline = time.monotonic() + 5
                while not (root / "port").exists():
                    test.assertLess(time.monotonic(), deadline)
                    time.sleep(.01)
                with socket.create_connection(("127.0.0.1", int((root / "port").read_text())), timeout=5) as original:
                    original.sendall(b"original")
                    test.assertEqual(original.recv(16), b"original")
                    # The original socket's EOF is the cleanup ACK; no second
                    # connection or elapsed sleep stands in for retirement.
                    original.shutdown(socket.SHUT_WR)
                try:
                    if mode == "wedged":
                        with test.assertRaisesRegex(module.SetupError, "^transport_cleanup_failed$"):
                            transport.close()
                    else:
                        transport.close()
                finally:
                    closed_by_owner = child.returncode is not None
                test.assertEqual(child.returncode, 143 if mode == "wedged" else 0)
                if mode == "natural":
                    test.assertEqual((root / "closed").read_text(), "closed")
                else:
                    test.assertFalse((root / "closed").exists())
            finally:
                # A close mutant must turn red without leaving a real
                # process behind. This fallback is outside the assertion.
                if child.poll() is None:
                    child.terminate()
                child.wait(timeout=15)
            test.assertTrue(closed_by_owner, "transport owner did not kill and wait its child")
            with test.assertRaises(ChildProcessError):
                os.waitpid(child.pid, os.WNOHANG)
