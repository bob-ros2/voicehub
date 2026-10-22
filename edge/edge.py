"""VoiceHub TTS edge: long-polls the hub and publishes to ROS 2 for agent-q3tts.

Runs on the TTS host next to agent-q3tts (leave that container untouched).
Config via env:
  VOICEHUB_URL   hub base URL (default http://127.0.0.1:8790)
  VOICEHUB_TOKEN shared bearer token
  TTS_TOPIC      absolute topic, must match the q3tts subscription remap
                 (default /agent/llm_stream)
  TTS_NODE_NS    namespace for this edge node (default /voicehub)
  ROS_DOMAIN_ID  DDS domain (must match agent-q3tts; default 55)
"""

import json
import os
import time
import urllib.error
import urllib.request

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
from std_msgs.msg import String

HUB = os.getenv("VOICEHUB_URL", "http://127.0.0.1:8790").rstrip("/")
TOKEN = os.getenv("VOICEHUB_TOKEN", "")
TOPIC = os.getenv("TTS_TOPIC", "/agent/llm_stream")
NS = os.getenv("TTS_NODE_NS", "/voicehub")
HEARTBEAT_S = 10.0


def http(method: str, path: str, payload=None, timeout: float = 10.0):
    req = urllib.request.Request(
        HUB + path,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    return json.loads(body) if body else {}


def main() -> None:
    rclpy.init()
    node = Node("voicehub_tts_edge", namespace=NS or None)
    qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE,
                     durability=DurabilityPolicy.VOLATILE)
    pub = node.create_publisher(String, TOPIC, qos)
    log = node.get_logger()
    domain = os.environ.get("ROS_DOMAIN_ID", "55")
    log.info(f"VoiceHub edge: hub={HUB} topic={TOPIC} ns={NS} domain={domain}")

    last_hb = 0.0
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.05)

        now = time.time()
        if now - last_hb > HEARTBEAT_S:
            try:
                subs = pub.get_subscription_count()
            except Exception:
                subs = -1
            try:
                http("POST", "/v1/tts/state", {
                    "topic": TOPIC, "namespace": NS, "domain": domain,
                    "subscribers": subs, "node": f"{NS}/{node.get_name()}",
                })
                last_hb = now
            except Exception as e:
                log.warn(f"hub heartbeat failed: {e}")

        try:
            jobs = http("GET", "/v1/tts/jobs?wait=2&max=10", timeout=8.0).get("jobs", [])
        except (urllib.error.URLError, OSError) as e:
            log.warn(f"hub unreachable: {e}")
            jobs = []
        if not jobs:
            continue

        for job in jobs:
            msg = String()
            msg.data = job["text"]
            pub.publish(msg)
            log.info(f"published job {job['id']}: {job['text'][:60]!r}")
        try:
            http("POST", "/v1/tts/ack", {"ids": [j["id"] for j in jobs]})
        except Exception as e:
            log.warn(f"ack failed: {e}")
        rclpy.spin_once(node, timeout_sec=0.05)


if __name__ == "__main__":
    main()
