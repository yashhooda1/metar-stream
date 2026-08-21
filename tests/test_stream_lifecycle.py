import signal
import threading
import unittest
from unittest.mock import patch

from metar_stream import (
    _request_shutdown,
    await_shutdown,
    install_signal_handlers,
    stop_queries,
)


class FakeQuery:
    def __init__(self, name, active=True, stop_error=None):
        self.name = name
        self.id = f"{name}-id"
        self.isActive = active
        self.stop_error = stop_error
        self.stop_calls = 0
        self.await_calls = []

    def stop(self):
        self.stop_calls += 1
        if self.stop_error:
            raise self.stop_error
        self.isActive = False

    def awaitTermination(self, timeout):
        self.await_calls.append(timeout)
        return True


class FakeStreams:
    def __init__(self, results):
        self.results = iter(results)
        self.polls = []

    def awaitAnyTermination(self, timeout):
        self.polls.append(timeout)
        result = next(self.results)
        if isinstance(result, Exception):
            raise result
        return result


class FakeSpark:
    def __init__(self, results):
        self.streams = FakeStreams(results)


class StreamLifecycleTests(unittest.TestCase):
    def test_signal_handler_requests_shutdown(self):
        stop_event = threading.Event()

        _request_shutdown(stop_event, signal.SIGTERM, None)

        self.assertTrue(stop_event.is_set())

    @patch("metar_stream.signal.signal")
    def test_installs_interrupt_and_termination_handlers(self, register):
        install_signal_handlers(threading.Event())

        self.assertEqual(
            [call.args[0] for call in register.call_args_list],
            [signal.SIGINT, signal.SIGTERM],
        )

    def test_await_shutdown_polls_until_signal(self):
        stop_event = threading.Event()
        spark = FakeSpark([False, False, True])

        await_shutdown(spark, stop_event, poll_seconds=0.25)

        self.assertEqual(spark.streams.polls, [0.25, 0.25, 0.25])

    def test_stop_queries_cleans_all_active_queries(self):
        active = FakeQuery("active")
        inactive = FakeQuery("inactive", active=False)

        stop_queries([active, inactive], timeout_seconds=7)

        self.assertEqual(active.stop_calls, 1)
        self.assertEqual(active.await_calls, [7])
        self.assertEqual(inactive.stop_calls, 0)

    def test_stop_queries_continues_after_one_failure(self):
        broken = FakeQuery("broken", stop_error=RuntimeError("stop failed"))
        healthy = FakeQuery("healthy")

        with self.assertLogs("metar-stream", level="ERROR"):
            stop_queries([broken, healthy])

        self.assertEqual(broken.stop_calls, 1)
        self.assertEqual(healthy.stop_calls, 1)


if __name__ == "__main__":
    unittest.main()
