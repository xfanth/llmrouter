#!/usr/bin/env python3
"""Checks for the harness capture fix (issues #28 and #29).

A fake chat server on 127.0.0.1 answers POSTs to /chat/completions with a
fixed JSON body carrying a usage block, or with SSE frames whose final frame
carries the usage block. The checks then verify that curl_stream returns the
parsed body, that tier_concurrent rows carry the token counts and snippets,
and that truncation respects character boundaries. Plain asserts, no
framework: run with `python3 scripts/test_redeploy_suite.py`.
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import redeploy_suite  # noqa: E402


USAGE = {"prompt_tokens": 10, "completion_tokens": 7}
JSON_BODY = json.dumps({
    "id": "test-1",
    "choices": [{"message": {"role": "assistant", "content": "5"}}],
    "usage": USAGE,
}).encode()


class FakeChatHandler(BaseHTTPRequestHandler):
    """Answers every POST with a canned completion; SSE when the request
    body asks for stream mode, a multibyte body for the truncation check."""

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        request = self.rfile.read(length).decode("utf-8")
        if '"big": true' in request:
            # The response deliberately exceeds the 2KB snippet cap and uses
            # a two-byte character, so a byte-based truncation would cut
            # through the middle of a code point.
            payload = json.dumps({
                "choices": [{"message": {"role": "assistant", "content": "é" * 3000}}],
                "usage": USAGE,
            }).encode()
            ctype = "application/json"
        elif '"stream": true' in request:
            payload = (
                'data: {"id":"t","choices":[{"delta":{"content":"5"}}]}\n\n'
                'data: {"id":"t","choices":[],"usage":' + json.dumps(USAGE) + "}\n\n"
                "data: [DONE]\n\n"
            ).encode()
            ctype = "text/event-stream"
        else:
            payload = JSON_BODY
            ctype = "application/json"
        self.send_response(200)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        pass  # keep the test output clean


def start_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeChatHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def expect_error(message, fn):
    """Run fn and assert it fails, for the red phase of the protocol."""
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - any failure counts as red
        print(f"  red as expected: {message} ({type(exc).__name__})")
        return
    raise AssertionError(f"expected a failure before the fix: {message}")


def test_curl_stream_returns_body(server_url):
    """curl_stream must return the parsed response and its usage block
    instead of discarding the body (issue #28)."""
    result = redeploy_suite.curl_stream(
        f"{server_url}/chat/completions",
        {"model": "m", "max_tokens": 8, "messages": [{"role": "user", "content": "hi"}]},
        None,
        30,
    )
    assert result["status"] == 200, f"status {result['status']}"
    parsed = result.get("parsed")
    assert isinstance(parsed, dict) and parsed.get("usage") == USAGE, f"parsed={parsed!r}"
    tokens_in, tokens_out = redeploy_suite._tokens_from_response(
        result.get("parsed"), result.get("raw") or "")
    assert (tokens_in, tokens_out) == (10, 7), f"tokens=({tokens_in}, {tokens_out})"
    print("  PASS curl_stream returns the body and usage")


def test_curl_stream_sse_usage(server_url):
    """For a streaming response the usage lives on the final data frame, so
    the extraction must find it there."""
    result = redeploy_suite.curl_stream(
        f"{server_url}/chat/completions",
        {"model": "m", "max_tokens": 8, "stream": True,
         "messages": [{"role": "user", "content": "hi"}]},
        None,
        30,
    )
    assert result["status"] == 200, f"status {result['status']}"
    tokens_in, tokens_out = redeploy_suite._tokens_from_response(
        result.get("parsed"), result.get("raw") or "")
    assert (tokens_in, tokens_out) == (10, 7), f"tokens=({tokens_in}, {tokens_out})"
    print("  PASS streaming usage read from the final data frame")


def test_tier_concurrent_captures(server_url):
    """Per-request rows carry status, snippets, tokens and tps; the summary
    carries tps statistics (issue #29)."""
    tier = redeploy_suite.tier_concurrent(f"{server_url}/chat/completions",
                                          None, "m", 2, 8)
    assert tier["pass"] is True, tier
    rows = tier["per_request"]
    assert len(rows) == 2, rows
    for row in rows:
        assert row["status"] == 200, row
        assert row["tokens_in"] == 10 and row["tokens_out"] == 7, row
        assert row["tps"] is not None and row["tps"] > 0, row
        assert row["request_snippet"].startswith('{"model"'), row
        assert row["response_snippet"].startswith("data: "), row
    summary_tps = tier.get("tps") or {}
    assert summary_tps.get("n") == 2 and summary_tps.get("median", 0) > 0, summary_tps
    print("  PASS tier_concurrent rows capture tokens, snippets and tps")


def test_snippet_truncation(server_url):
    """Snippets must be truncated on character boundaries and stay inside
    the 2KB cap, for a body longer than the cap."""
    result = redeploy_suite.curl_stream(
        f"{server_url}/chat/completions",
        {"big": True},
        None,
        30,
    )
    snippet = redeploy_suite._snippet(result.get("raw"))
    assert snippet is not None and 0 < len(snippet) <= redeploy_suite.SNIPPET_CAP, len(snippet)
    # A byte-based cut through a two-byte character yields U+FFFD; Python
    # string slicing never does, whatever the characters in the body.
    assert "�" not in snippet, "broken characters in snippet"
    direct = redeploy_suite._snippet("é" * 3000)
    assert direct is not None and len(direct) == redeploy_suite.SNIPPET_CAP, len(direct)
    assert direct[-1] == "é", "character cut in half"
    print("  PASS snippets truncate on character boundaries")


def main():
    server = start_server()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}"
        red = "--red" in sys.argv
        red_checks = [
            ("curl_stream body capture", lambda: test_curl_stream_returns_body(url)),
            ("streaming usage extraction", lambda: test_curl_stream_sse_usage(url)),
            ("tier_concurrent capture fields", lambda: test_tier_concurrent_captures(url)),
        ]
        if red:
            for message, check in red_checks:
                expect_error(message, check)
            print("RED PHASE OK: all capture checks fail before the fix")
            return 0
        test_curl_stream_returns_body(url)
        test_curl_stream_sse_usage(url)
        test_tier_concurrent_captures(url)
        test_snippet_truncation(url)
        print("ALL PASS")
        return 0
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    sys.exit(main())
