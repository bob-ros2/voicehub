# VoiceHub — Integration recipe for coding agents

You are a coding agent and are to integrate VoiceHub into your agent as an
extension. This document is the complete build recipe; everything else is
reference: `GET /` (service list), `/docs` (Swagger), `/openapi.json` (spec).

## What VoiceHub is

An HTTP service (default port 8790) that mediates between you and the voice
hardware. You NEVER talk to ROS 2, Docker, FIFOs, or microphones directly.

```
YOUR EXTENSION                      VoiceHub (http://HOST:8790)
├─ TTS tool  ──── POST /v1/speak ──► Queue → ROS → Qwen3-TTS → Speaker
├─ STT client ◄── SSE /v1/stt/stream ── Transcripts from the microphone container
└─ Status tool ── GET /v1/status ───► Health of TTS/STT/consumers
```

Auth: all `/v1/*` want `Authorization: Bearer $VOICEHUB_TOKEN`
(token from the operator, in the hub's `.env`). No-token
exceptions: `/health`, `/`, `/docs`, `/openapi.json`, `/integration`.

## What you must build — exactly three parts

### 1. TTS tool ("speak")

Expose a tool `tts_speak` to the agent (text parameter). execute = one POST:

```
POST /v1/speak
{"text": "Done. The test run finished."}
→ 202 {"ok":true,"ids":[7]}
```

Fire and forget (202 is enough; do not wait for audio). Lines in `text`
become individual voice messages. At-most-once: if no TTS host is attached,
the hub silently drops after 30 s — then report "sent", not "spoken".

### 2. STT client (the interesting part)

When your extension starts, open a **persistent connection** to the SSE
stream and inject every event as a **user message into the current session**
(e.g. `pi.sendUserMessage(text)`; for omp: idle → prompt, streaming → steer).

- Endpoint: `GET /v1/stt/stream`, response `text/event-stream`:
  ```
  id: 42
  data: {"id":42,"text":"hello world","ts":1792696281.3,"source":"vox"}
  ```
- You set the Bearer header yourself — **no browser EventSource** (it cannot
  send headers). Use fetch/reqwest with stream reading and parse `id:`/`data:`
  lines (end of event = blank line). The hub sends a `:ping` every 15 s as a
  keepalive.
- **Reconnect is mandatory**: on disconnect, reconnect with backoff
  (e.g. 1s,2s,4s, max 30s) and pass the last seen id:
  `GET /v1/stt/stream?since=42` → the hub replays up to 20 missed events,
  so you lose no dictation across restarts.
- Broadcast: with several agents running, **each one** gets every transcript.
  If that is a problem, mute via a config switch — the hub does not decide
  that for you.
- The agent may be busy: injection must also work during a running turn
  (steer/queue depending on the extension API).

### 3. Status tool

Tool `voice_status`, execute = `GET /v1/status`, pass the JSON to the agent.
Rules of thumb:
- `vox.last_heartbeat_age_s > 60` ⇒ microphone side dead.
- `tts.edge_state.subscribers < 1` ⇒ no TTS receiver (nothing will speak).
- `stt.consumers` = how many agents are listening.

## Configuration — only two values

```
VOICEHUB_URL=http://HOST:8790      # default
VOICEHUB_TOKEN=<from the operator> # Bearer for /v1/*
```

## Verification (in this order, without your extension)

```sh
curl -s http://HOST:8790/health                                  # {"ok":true}
curl -s -H "Authorization: Bearer $T" http://HOST:8790/v1/status # subs>=1, vox_hb<60s
curl -s -H "Authorization: Bearer $T" http://HOST:8790/v1/stt/recent
curl -N -H "Authorization: Bearer $T" http://HOST:8790/v1/stt/stream &   # listen
curl -s -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
     -X POST http://HOST:8790/v1/stt -d '{"text":"test","source":"manual"}'
# → the event must appear in the open stream
```

Acceptance criteria AFTER your integration:
1. `tts_speak("connection test")` ⇒ speech from the speaker.
2. Speak into the microphone ⇒ within ~3 s the text appears as a
   user message in your agent session.
3. Restart the extension, use `?since=` ⇒ the last transcript is
   replayed (no prompt lost).
4. `voice_status` explains in one line what is broken if something is silent.

## What you must NOT do

- No ROS 2, no FIFOs, no `docker exec`,
  no direct microphone/speaker handling. VoiceHub is the only boundary.
- Do not start your own containers; the human runs hub/vox/edge.
- `/v1/stt` and `/v1/vox/heartbeat` are ONLY for the hardware side — your
  agent must NEVER call them (otherwise you would simulate voice input).
