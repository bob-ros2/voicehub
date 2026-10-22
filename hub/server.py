"""VoiceHub — agent-neutral API layer between agents and TTS/STT containers.

Containers (any hosts): hub=this HTTP API (no ROS, no audio); edge=long-polls
TTS jobs and publishes ROS 2 next to agent-q3tts; vox=vox-real+handler, POSTs
transcripts. All Bearer-auth with shared VOICEHUB_TOKEN (except /health).

  POST /v1/speak            {"text", "target"?}        agent -> TTS queue
  GET  /v1/tts/jobs         ?wait=2&max=10             edge long-poll
  POST /v1/tts/ack          {"ids": [..]}              edge
  POST /v1/tts/state        {topic,domain,subscribers} edge heartbeat
  POST /v1/stt              {"text", "source"?}       vox prompt callback
  POST /v1/vox/heartbeat    {}                        vox
  GET  /v1/stt/stream       SSE (Last-Event-ID resume) agent
  GET  /v1/stt/recent       ?limit=20                  agent replay
  GET  /v1/status           aggregate                  agent
  GET  /health              no auth
"""

import asyncio
import collections
import itertools
import json
import os
import threading
import time
from typing import Any

from aiohttp import web

TOKEN = os.getenv("VOICEHUB_TOKEN", "").strip()
BIND = os.getenv("VOICEHUB_BIND", "127.0.0.1")
PORT = int(os.getenv("VOICEHUB_PORT", "8790"))
TTS_MAX = int(os.getenv("VOICEHUB_TTS_MAX", "100"))
TTS_TTL = float(os.getenv("VOICEHUB_TTS_TTL", "30"))
STT_MAX = int(os.getenv("VOICEHUB_STT_MAX", "20"))
STARTED = time.time()
ROS_TTS = os.getenv("VOICEHUB_ROS_TTS", "1") == "1"
TTS_TOPIC = os.getenv("TTS_TOPIC", "/agent/llm_stream")
TTS_NODE_NS = os.getenv("TTS_NODE_NS", "/voicehub")


class Hub:
    def __init__(self) -> None:
        self.tts_pending: collections.deque[dict[str, Any]] = collections.deque()
        self.tts_delivered: collections.deque[dict[str, Any]] = collections.deque(maxlen=50)
        self.tts_wake = asyncio.Event()
        self.tts_id = itertools.count(1)
        self.tts_state: dict[str, Any] | None = None
        self.tts_state_at = 0.0
        self.vox_last_seen = 0.0
        self.stt: collections.deque[dict[str, Any]] = collections.deque(maxlen=STT_MAX)
        self.stt_id = itertools.count(1)
        self.consumers: set[asyncio.Queue] = set()
        self.dropped = 0

    # --- TTS ---------------------------------------------------------------

    def speak(self, text: str, target: str | None) -> list[int]:
        ids = []
        for line in (s.strip() for s in text.splitlines()):
            if not line:
                continue
            jid = next(self.tts_id)
            ids.append(jid)
            self.tts_pending.append({"id": jid, "text": line, "ts": time.time(), "target": target})
        while len(self.tts_pending) > TTS_MAX:
            self.tts_pending.popleft()
            self.dropped += 1
        if ids:
            self.tts_wake.set()
        return ids

    def _drop_expired(self) -> None:
        now = time.time()
        while self.tts_pending and now - self.tts_pending[0]["ts"] > TTS_TTL:
            self.tts_pending.popleft()
            self.dropped += 1

    async def take_jobs(self, wait: float, max_jobs: int) -> list[dict[str, Any]]:
        deadline = time.time() + wait
        self._drop_expired()
        while not self.tts_pending and time.time() < deadline:
            self.tts_wake.clear()
            self._drop_expired()
            if self.tts_pending:
                break
            try:
                await asyncio.wait_for(self.tts_wake.wait(), timeout=deadline - time.time())
            except asyncio.TimeoutError:
                break
        jobs = [self.tts_pending.popleft() for _ in range(min(max_jobs, len(self.tts_pending)))]
        for j in jobs:
            self.tts_delivered.append({"id": j["id"], "delivered_at": time.time(), "acked_at": None})
        return jobs

    def ack(self, ids: list[int]) -> None:
        wanted = set(ids)
        for rec in self.tts_delivered:
            if rec["id"] in wanted and rec["acked_at"] is None:
                rec["acked_at"] = time.time()

    # --- STT ---------------------------------------------------------------

    def add_transcript(self, text: str, source: str) -> dict[str, Any]:
        ev = {"id": next(self.stt_id), "text": text, "ts": time.time(), "source": source}
        self.stt.append(ev)
        for q in list(self.consumers):
            try:
                q.put_nowait(ev)
            except asyncio.QueueFull:
                self.consumers.discard(q)  # consumer too slow: drop it
        return ev


HUB = Hub()


# --- HTTP handlers ----------------------------------------------------------

async def h_speak(req: web.Request) -> web.Response:
    body = await _json(req)
    text = body.get("text")
    if not isinstance(text, str) or not text.strip():
        return _err("empty text", 400)
    target = body.get("target")
    ids = HUB.speak(text, target if isinstance(target, str) else None)
    return web.json_response({"ok": True, "ids": ids}, status=202)


async def h_tts_jobs(req: web.Request) -> web.Response:
    wait = min(60.0, max(0.0, float(req.query.get("wait", "2"))))
    max_jobs = min(50, max(1, int(req.query.get("max", "10"))))
    jobs = await HUB.take_jobs(wait, max_jobs)
    return web.json_response({"jobs": jobs})


async def h_tts_ack(req: web.Request) -> web.Response:
    body = await _json(req)
    ids = body.get("ids") or []
    if not isinstance(ids, list):
        return _err("ids must be a list", 400)
    HUB.ack([int(i) for i in ids])
    return web.json_response({"ok": True})


async def h_tts_state(req: web.Request) -> web.Response:
    body = await _json(req)
    HUB.tts_state = body
    HUB.tts_state_at = time.time()
    return web.json_response({"ok": True})


async def h_stt(req: web.Request) -> web.Response:
    body = await _json(req)
    text = body.get("text")
    if not isinstance(text, str) or not text.strip():
        return _err("empty text", 400)
    source = body.get("source") if isinstance(body.get("source"), str) else "vox"
    ev = HUB.add_transcript(text.strip(), source)
    return web.json_response({"ok": True, "id": ev["id"]})


async def h_vox_heartbeat(req: web.Request) -> web.Response:
    HUB.vox_last_seen = time.time()
    return web.json_response({"ok": True})


async def h_stt_recent(req: web.Request) -> web.Response:
    limit = min(STT_MAX, max(1, int(req.query.get("limit", "20"))))
    return web.json_response({"events": list(HUB.stt)[-limit:]})


async def h_stt_stream(req: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache",
                 "X-Accel-Buffering": "no"},
    )
    await resp.prepare(req)
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    HUB.consumers.add(q)
    try:
        last = req.headers.get("Last-Event-ID") or req.query.get("since")
        if last is not None:
            for ev in HUB.stt:
                if ev["id"] > int(last):
                    await _sse(resp, ev)
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=15)
            except asyncio.TimeoutError:
                await resp.write(b":ping\n\n")
                continue
            await _sse(resp, ev)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        HUB.consumers.discard(q)
    return resp


async def h_status(req: web.Request) -> web.Response:
    now = time.time()
    state_age = now - HUB.tts_state_at if HUB.tts_state_at else None
    return web.json_response({
        "ok": True,
        "uptime_s": round(now - STARTED, 1),
        "tts": {
            "pending": len(HUB.tts_pending),
            "dropped": HUB.dropped,
            "edge_state": HUB.tts_state,
            "edge_state_age_s": round(state_age, 1) if state_age is not None else None,
            "delivered_recent": list(HUB.tts_delivered)[-5:],
        },
        "vox": {"last_heartbeat_age_s": round(now - HUB.vox_last_seen, 1) if HUB.vox_last_seen else None},
        "stt": {"buffered": len(HUB.stt), "last": HUB.stt[-1] if HUB.stt else None,
                "consumers": len(HUB.consumers)},
    })


async def h_health(req: web.Request) -> web.Response:
    return web.json_response({"ok": True})


API_DOCS: dict[str, Any] = {
    "service": "voicehub",
    "version": "1",
    "start_here": "GET /integration — complete build recipe for coding agents",
    "docs": "docs/API.md in the repo; interactive: GET /docs",
    "integration": "GET /integration",
    "auth": "Authorization: Bearer $VOICEHUB_TOKEN on all /v1/* except /health",
    "tts_mode": "internal-ros" if ROS_TTS else "edge-poll",
    "endpoints": [
        {"method": "POST", "path": "/v1/speak", "who": "agent",
         "what": "speak text; lines -> one ROS String each"},
        {"method": "GET", "path": "/v1/tts/jobs?wait=2&max=10", "who": "edge (split-host only)",
         "what": "long-poll pending TTS jobs"},
        {"method": "POST", "path": "/v1/tts/ack", "who": "edge", "what": "confirm delivered ids"},
        {"method": "POST", "path": "/v1/tts/state", "who": "edge", "what": "heartbeat: topic/domain/subscribers"},
        {"method": "POST", "path": "/v1/stt", "who": "vox handler",
         "what": "prompt callback: transcript text"},
        {"method": "GET", "path": "/v1/stt/stream", "who": "agent", "what": "SSE transcript fan-out (Last-Event-ID resume)"},
        {"method": "GET", "path": "/v1/stt/recent?limit=20", "who": "agent", "what": "replay buffer"},
        {"method": "GET", "path": "/v1/status", "who": "agent", "what": "aggregate health of tts/vox/consumers"},
        {"method": "GET", "path": "/health", "who": "any", "what": "liveness, no auth"},
    ],
}


async def h_root(req: web.Request) -> web.Response:
    return web.json_response(API_DOCS)


OPENAPI: dict[str, Any] = {
    "openapi": "3.0.3",
    "info": {"title": "VoiceHub API", "version": "1",
             "description": "Agent-neutral TTS/STT layer. All /v1/* need Authorization: "
                            "Bearer $VOICEHUB_TOKEN (use the Authenticate button).\n\n"
                            "**Start here:** complete build recipe for coding agents: "
                            "[GET /integration](/integration). "
                            "Service list as JSON: [GET /](/)."},
    "security": [{"bearerAuth": []}],
    "paths": {
        "/v1/speak": {"post": {
            "summary": "Speak text (agent)", "description": "Lines become one ROS String message each.",
            "requestBody": {"required": True, "content": {"application/json": {"schema": {
                "type": "object", "required": ["text"],
                "properties": {"text": {"type": "string"}, "target": {"type": "string"}}}}}},
            "responses": {"202": {"description": "{\"ok\":true,\"ids\":[..]}"}}}},
        "/v1/tts/jobs": {"get": {"summary": "Long-poll pending TTS jobs (voicehub-edge only)",
            "description": "Empty in internal-ros mode.",
            "parameters": [{"name": "wait", "in": "query", "schema": {"type": "number", "default": 2}},
                           {"name": "max", "in": "query", "schema": {"type": "number", "default": 10}}],
            "responses": {"200": {"description": "{\"jobs\":[..]}"}}}},
        "/v1/tts/ack": {"post": {"summary": "Ack delivered job ids (edge only)",
            "requestBody": {"required": True, "content": {"application/json": {"schema": {
                "type": "object", "properties": {"ids": {"type": "array", "items": {"type": "integer"}}}}}}},
            "responses": {"200": {"description": "{\"ok\":true}"}}}},
        "/v1/tts/state": {"post": {"summary": "Edge heartbeat: topic/domain/subscribers",
            "requestBody": {"content": {"application/json": {"schema": {"type": "object"}}}},
            "responses": {"200": {"description": "{\"ok\":true}"}}}},
        "/v1/stt": {"post": {"summary": "Prompt callback: transcript from vox handler",
            "requestBody": {"required": True, "content": {"application/json": {"schema": {
                "type": "object", "required": ["text"],
                "properties": {"text": {"type": "string"}, "source": {"type": "string"}}}}}},
            "responses": {"200": {"description": "{\"ok\":true,\"id\":N}"}}}},
        "/v1/vox/heartbeat": {"post": {"summary": "Vox liveness (every 30 s)",
            "responses": {"200": {"description": "{\"ok\":true}"}}}},
        "/v1/stt/stream": {"get": {"summary": "SSE transcript fan-out (agents)",
            "description": "text/event-stream; resume via Last-Event-ID header or ?since=<id>.",
            "responses": {"200": {"description": "SSE stream"}}}},
        "/v1/stt/recent": {"get": {"summary": "Replay buffer (last 20)",
            "parameters": [{"name": "limit", "in": "query", "schema": {"type": "number", "default": 20}}],
            "responses": {"200": {"description": "{\"events\":[..]}"}}}},
        "/v1/status": {"get": {"summary": "Aggregate health (tts/vox/consumers)",
            "responses": {"200": {"description": "status object"}}}},
        "/health": {"get": {"summary": "Liveness, no auth", "security": [],
            "responses": {"200": {"description": "{\"ok\":true}"}}}},
    },
    "components": {"securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}},
}

DOC_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>VoiceHub API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css"></head>
<body><div id="swagger"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url:"/openapi.json",dom_id:"#swagger"});</script></body></html>"""


async def h_openapi(req: web.Request) -> web.Response:
    return web.json_response(OPENAPI)


async def h_docs(req: web.Request) -> web.Response:
    return web.Response(text=DOC_HTML, content_type="text/html")

async def h_integration(req: web.Request) -> web.Response:
    path = os.environ.get("VOICEHUB_DOCS_FILE", "/docs/AGENT_INTEGRATION.md")
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except OSError:
        text = ("VoiceHub: integration recipe not mounted "
                "(volume ../docs -> /docs). In the repo: docs/AGENT_INTEGRATION.md")
    return web.Response(text=text, content_type="text/markdown", charset="utf-8")

# --- plumbing ----------------------------------------------------------------

async def _json(req: web.Request) -> dict[str, Any]:
    try:
        data = await req.json()
    except Exception:
        raise web.HTTPBadRequest(text='{"error":"invalid json"}', content_type="application/json") from None
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text='{"error":"body must be an object"}', content_type="application/json")
    return data


def _err(msg: str, status: int) -> web.Response:
    return web.json_response({"error": msg}, status=status)


async def _sse(resp: web.StreamResponse, ev: dict[str, Any]) -> None:
    await resp.write(f"id: {ev['id']}\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n".encode())


@web.middleware
async def auth(req: web.Request, handler) -> web.Response:
    if TOKEN and req.path.startswith("/v1/"):
        if req.headers.get("authorization") != f"Bearer {TOKEN}":
            return _err("unauthorized", 401)
    return await handler(req)


# --- internal ROS TTS mode (hub sits on the TTS host; no edge container) ---

def start_ros_tts() -> bool:
    """Publish queued speak jobs straight onto the ROS 2 topic from this
    process. False if rclpy is missing — then run the optional voicehub-edge
    container (it consumes GET /v1/tts/jobs instead)."""
    try:
        import rclpy
        from rclpy.node import Node
        from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
        from std_msgs.msg import String
    except ImportError:
        return False

    def loop() -> None:
        rclpy.init()
        node = Node("voicehub_tts", namespace=TTS_NODE_NS or None)
        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE,
                         durability=DurabilityPolicy.VOLATILE)
        pub = node.create_publisher(String, TTS_TOPIC, qos)
        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=0.02)
            HUB._drop_expired()
            job = HUB.tts_pending.popleft() if HUB.tts_pending else None
            if job is not None:
                msg = String()
                msg.data = job["text"]
                pub.publish(msg)
                t = time.time()
                HUB.tts_delivered.append({"id": job["id"], "delivered_at": t, "acked_at": t})
            if time.time() - HUB.tts_state_at > 10:
                try:
                    subs = pub.get_subscription_count()
                except Exception:
                    subs = -1
                HUB.tts_state = {"mode": "internal", "topic": TTS_TOPIC, "namespace": TTS_NODE_NS,
                                 "domain": os.environ.get("ROS_DOMAIN_ID", "55"), "subscribers": subs}
                HUB.tts_state_at = time.time()

    threading.Thread(target=loop, daemon=True).start()
    return True


def main() -> None:
    ros_ok = start_ros_tts() if ROS_TTS else False
    if ROS_TTS and not ros_ok:
        print("VOICEHUB_ROS_TTS=1 but rclpy is unavailable: run voicehub-edge instead", flush=True)
    app = web.Application(middlewares=[auth])
    app.router.add_get("/health", h_health)
    app.router.add_post("/v1/speak", h_speak)
    app.router.add_get("/v1/tts/jobs", h_tts_jobs)
    app.router.add_post("/v1/tts/ack", h_tts_ack)
    app.router.add_post("/v1/tts/state", h_tts_state)
    app.router.add_post("/v1/stt", h_stt)
    app.router.add_post("/v1/vox/heartbeat", h_vox_heartbeat)
    app.router.add_get("/", h_root)
    app.router.add_get("/integration", h_integration)
    app.router.add_get("/openapi.json", h_openapi)
    app.router.add_get("/docs", h_docs)
    app.router.add_get("/v1/stt/stream", h_stt_stream)
    app.router.add_get("/v1/stt/recent", h_stt_recent)
    app.router.add_get("/v1/status", h_status)
    web.run_app(app, host=BIND, port=PORT, print=lambda *a: None)


if __name__ == "__main__":
    main()
