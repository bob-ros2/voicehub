"""Integration tests for the VoiceHub hub.

Every test runs against a real `hub/server.py` subprocess in split-host mode
(VOICEHUB_ROS_TTS=0, no ROS needed) driven over plain HTTP — the same surface
agents, vox and edge see.
"""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

SERVER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "hub", "server.py")
TOKEN = "test-token-42"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Hub:
    """Small client for one running hub instance."""

    def __init__(self, proc: subprocess.Popen, base: str) -> None:
        self.proc, self.base = proc, base

    def req(self, method: str, path: str, body=None, token: str | None = TOKEN):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        if token:
            r.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                return resp.status, json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"null")

    def open_stream(self, path: str, token: str | None = TOKEN, headers=None):
        r = urllib.request.Request(self.base + path, method="GET")
        if token:
            r.add_header("Authorization", f"Bearer {token}")
        for k, v in (headers or {}).items():
            r.add_header(k, v)
        return urllib.request.urlopen(r, timeout=5)

    def status(self) -> dict:
        code, body = self.req("GET", "/v1/status")
        assert code == 200
        return body


def start_hub(**env_over):
    port = _free_port()
    env = {
        **os.environ,
        "VOICEHUB_TOKEN": TOKEN,
        "VOICEHUB_BIND": "127.0.0.1",
        "VOICEHUB_PORT": str(port),
        "VOICEHUB_ROS_TTS": "0",
    }
    env.update({k: str(v) for k, v in env_over.items()})
    proc = subprocess.Popen([sys.executable, SERVER], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 5
    while True:
        try:
            urllib.request.urlopen(base + "/health", timeout=1)
            break
        except OSError:
            if proc.poll() is not None:
                raise RuntimeError(f"hub exited: {proc.stdout.read()}") from None
            if time.time() > deadline:
                raise RuntimeError("hub did not start in time") from None
            time.sleep(0.05)
    return Hub(proc, base)


def _stop(h: Hub) -> None:
    # A live SSE handler keeps aiohttp's graceful shutdown busy until its
    # next ping; escalate to kill instead of waiting it out.
    h.proc.terminate()
    try:
        h.proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        h.proc.kill()
        h.proc.wait(timeout=3)


@pytest.fixture
def hub():
    h = start_hub()
    try:
        yield h
    finally:
        _stop(h)


# --- auth -------------------------------------------------------------------

def test_health_is_open(hub):
    assert hub.req("GET", "/health", token=None)[0] == 200


def test_root_is_open_and_reports_edge_poll_mode(hub):
    code, body = hub.req("GET", "/", token=None)
    assert code == 200
    assert body["tts_mode"] == "edge-poll"


def test_v1_requires_bearer_token(hub):
    code, body = hub.req("GET", "/v1/status", token=None)
    assert code == 401 and body["error"] == "unauthorized"
    assert hub.req("GET", "/v1/status", token="wrong")[0] == 401
    assert hub.req("GET", "/v1/status")[0] == 200


# --- TTS queue ---------------------------------------------------------------

def test_speak_splits_lines_and_strips(hub):
    code, body = hub.req("POST", "/v1/speak", {"text": "one\n\n  two  \n"})
    assert code == 202 and len(body["ids"]) == 2
    code, jobs = hub.req("GET", "/v1/tts/jobs?wait=0")
    assert code == 200
    assert [j["text"] for j in jobs["jobs"]] == ["one", "two"]
    assert [j["id"] for j in jobs["jobs"]] == body["ids"]


def test_speak_rejects_empty_text(hub):
    assert hub.req("POST", "/v1/speak", {"text": "   "})[0] == 400
    assert hub.req("POST", "/v1/speak", {})[0] == 400


def test_jobs_are_consumed_once(hub):
    hub.req("POST", "/v1/speak", {"text": "only"})
    first = hub.req("GET", "/v1/tts/jobs?wait=0")[1]["jobs"]
    second = hub.req("GET", "/v1/tts/jobs?wait=0")[1]["jobs"]
    assert len(first) == 1 and second == []


def test_ack_marks_delivered_jobs(hub):
    jid = hub.req("POST", "/v1/speak", {"text": "ack me"})[1]["ids"][0]
    hub.req("GET", "/v1/tts/jobs?wait=0")
    assert hub.req("POST", "/v1/tts/ack", {"ids": [jid]})[0] == 200
    rec = [d for d in hub.status()["tts"]["delivered_recent"] if d["id"] == jid][0]
    assert rec["acked_at"] is not None


def test_expired_jobs_are_dropped(hub_env_ttl):
    hub_env_ttl.req("POST", "/v1/speak", {"text": "stale"})
    time.sleep(0.4)
    assert hub_env_ttl.req("GET", "/v1/tts/jobs?wait=0")[1]["jobs"] == []
    st = hub_env_ttl.status()["tts"]
    assert st["pending"] == 0 and st["dropped"] == 1


def test_queue_capped_by_tts_max(hub_env_max):
    ids = hub_env_max.req("POST", "/v1/speak",
                          {"text": "\n".join(str(i) for i in range(5))})[1]["ids"]
    assert len(ids) == 5
    st = hub_env_max.status()["tts"]
    assert st["pending"] == 3 and st["dropped"] == 2


@pytest.fixture
def hub_env_ttl():
    h = start_hub(VOICEHUB_TTS_TTL=0.2)
    try:
        yield h
    finally:
        _stop(h)


@pytest.fixture
def hub_env_max():
    h = start_hub(VOICEHUB_TTS_MAX=3)
    try:
        yield h
    finally:
        _stop(h)


# --- STT ---------------------------------------------------------------------

def test_stt_ingest_assigns_ids_and_source(hub):
    a = hub.req("POST", "/v1/stt", {"text": "hallo"})
    b = hub.req("POST", "/v1/stt", {"text": "  zweiter  ", "source": "mic2"})
    assert a[0] == 200 and b[1]["id"] == a[1]["id"] + 1
    recent = hub.req("GET", "/v1/stt/recent")[1]["events"]
    assert recent[-1] == {**recent[-1], "text": "zweiter", "source": "mic2"}
    assert recent[-2]["source"] == "vox"


def test_stt_rejects_empty_text(hub):
    assert hub.req("POST", "/v1/stt", {"text": ""})[0] == 400


def test_recent_limit_keeps_order(hub):
    for i in range(3):
        hub.req("POST", "/v1/stt", {"text": f"t{i}"})
    events = hub.req("GET", "/v1/stt/recent?limit=2")[1]["events"]
    assert [e["text"] for e in events] == ["t1", "t2"]


def test_recent_ring_buffer_drops_oldest():
    h = start_hub(VOICEHUB_STT_MAX=4)
    try:
        for i in range(6):
            h.req("POST", "/v1/stt", {"text": f"t{i}"})
        events = h.req("GET", "/v1/stt/recent")[1]["events"]
        assert [e["text"] for e in events] == ["t2", "t3", "t4", "t5"]
        assert h.status()["stt"]["buffered"] == 4
    finally:
        _stop(h)


# --- SSE stream ---------------------------------------------------------------

def _read_sse_event(fp) -> dict:
    """Read one `id: N\\ndata: {...}\\n\\n` frame, skipping `:ping` comments."""
    ev_id, ev_data = None, None
    while True:
        line = fp.readline().decode("utf-8")
        if not line:
            raise AssertionError("SSE stream closed")
        line = line.rstrip("\n")
        if line.startswith("id: "):
            ev_id = int(line[4:])
        elif line.startswith("data: "):
            ev_data = json.loads(line[6:])
        elif not line:
            if ev_data is not None:
                assert ev_data["id"] == ev_id
                return ev_data
            ev_id = None


def test_sse_delivers_live_events(hub):
    with hub.open_stream("/v1/stt/stream") as fp:
        hub.req("POST", "/v1/stt", {"text": "live1"})
        hub.req("POST", "/v1/stt", {"text": "live2"})
        assert _read_sse_event(fp)["text"] == "live1"
        assert _read_sse_event(fp)["text"] == "live2"


def test_sse_replays_missed_events_via_last_event_id(hub):
    first = hub.req("POST", "/v1/stt", {"text": "seen"})[1]["id"]
    hub.req("POST", "/v1/stt", {"text": "missed"})
    with hub.open_stream("/v1/stt/stream", headers={"Last-Event-ID": str(first)}) as fp:
        assert _read_sse_event(fp)["text"] == "missed"


def test_sse_requires_token(hub):
    with pytest.raises(urllib.error.HTTPError) as exc:
        hub.open_stream("/v1/stt/stream", token=None)
    assert exc.value.code == 401


# --- misc contract -------------------------------------------------------------

def test_vox_heartbeat_shows_in_status(hub):
    assert hub.status()["vox"]["last_heartbeat_age_s"] is None
    assert hub.req("POST", "/v1/vox/heartbeat", {})[0] == 200
    assert hub.status()["vox"]["last_heartbeat_age_s"] < 2
