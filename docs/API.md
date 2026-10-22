# VoiceHub API (v1)

Base: `http://HOST:8790`. Auth: `Authorization: Bearer $VOICEHUB_TOKEN` on all
`/v1/*` paths (only `/health` is open). Content-Type: `application/json`,
UTF-8. Errors: `{"error": "…"}` with an appropriate HTTP status.

### GET /   (no token)
Machine-readable service list: service name, `tts_mode` (`internal-ros` /
`edge-poll`), auth hint, and all endpoints with method/description.

### GET /docs   +   GET /openapi.json   (no token)
Interactive Swagger UI (OpenAPI 3.0) with "Try it out"; token via the
Authorize button (Bearer). Note: `GET /v1/stt/stream` cannot be meaningfully
tested there (SSE stays open) — use `curl -N` for that.
Assets come from the unpkg CDN; on offline hub hosts, serve swagger-ui-dist
statically from the image if needed.

## TTS (agent → hub → ROS 2 → agent-q3tts)

Default: the hub publishes to the ROS topic itself (internal rclpy loop,
`VOICEHUB_ROS_TTS=1`). The three edge endpoints below exist only in
split-host mode (`VOICEHUB_ROS_TTS=0` + `voicehub-edge` container).

### POST /v1/speak
```json
{"text": "One or two spoken sentences.\nSecond line = second message."}
```
→ `202 {"ok": true, "ids": [3]}`. Lines become individual messages on the ROS
topic; q3TTS splits by sentences itself. Optional `"target": "default"`
(reserved for multi-host routing, currently ignored). At-most-once delivery;
jobs older than 30 s are dropped when no edge is connected.

### GET /v1/tts/jobs?wait=2&max=10        (edge only)
Long-poll; → `{"jobs": [{"id":3,"text":"…","ts":…,"target":null}]}` (may be `[]`).
(split-host mode only; empty in default mode since the hub drains its own queue)

### POST /v1/tts/ack  `{"ids": [3]}`      (edge only) → `{"ok": true}`
### POST /v1/tts/state  (edge heartbeat every 10 s)
`{"topic":"/agent/llm_stream","namespace":"/voicehub","domain":"55","subscribers":1,"node":"/voicehub/voicehub_tts_edge"}`

## STT (vox → hub → agent)

### POST /v1/stt   (prompt callback, from the vox handler)
```json
{"text": "recognized speech text", "source": "vox"}
```
→ `200 {"ok": true, "id": 42}`. The hub buffers the last 20 events and
fans out to all SSE consumers (broadcast; each extension filters itself).

### GET /v1/vox/heartbeat  (POST, every 30 s from the handler) → `{"ok": true}`

### GET /v1/stt/stream     (agent, SSE)
`text/event-stream`, one event per transcript:
```
id: 42
data: {"id":42,"text":"hello","ts":1792694582.0,"source":"vox"}
```
Comment `ping` every 15 s. Reconnect with a `Last-Event-ID` header or
`?since=<id>` replays buffered events (max 20). Multiple consumers each get
every event (broadcast). The client sets the Bearer header itself
(fetch/reqwest, not the browser EventSource).

### GET /v1/stt/recent?limit=20
→ `{"events":[…]}` — replay without an open connection.

## Status

### GET /v1/status
```json
{"ok": true, "uptime_s": 12,
 "tts": {"pending": 0, "dropped": 0, "edge_state": {…}, "edge_state_age_s": 1.1,
         "delivered_recent": [{"id":3,"delivered_at":…,"acked_at":…}]},
 "vox": {"last_heartbeat_age_s": 11.7},
 "stt": {"buffered": 1, "last": {…}, "consumers": 1}}
```
Rules of thumb: `edge_state_age_s > 15` ⇒ edge offline; `subscribers < 1` ⇒
agent-q3tts is not listening; `vox.last_heartbeat_age_s > 60` ⇒ vox container dead.

### GET /health → `{"ok": true}` (no token)

## Extension build recipe (pi/omp)

1. `tts_speak` tool → `POST /v1/speak`.
2. On session start, open SSE `/v1/stt/stream` (fetch + ReadableStream,
   remember `Last-Event-ID`); per event, inject the text as a user message into
   the session (`pi.sendUserMessage` in omp; idle → prompt, streaming → steer).
   Reconnect with backoff; `?since=<lastId>` prevents lost prompts.
3. Status tool → `GET /v1/status`.
4. Config is only two values: `VOICEHUB_URL`, `VOICEHUB_TOKEN`.

Never block: speak calls are fire-and-forget (202), STT events are async —
the extension must handle agent-busy.
