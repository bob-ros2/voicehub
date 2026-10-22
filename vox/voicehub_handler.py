"""VoiceHub vox output handler: forwards STT transcripts to the hub.

Loaded by `vox --custom-handler /app/voicehub_handler.py`. Unknown CLI args
after the known vox options are parsed here (handler_args).

Config (env or CLI):
  VOICEHUB_URL  hub base URL (default http://127.0.0.1:8790)
  VOICEHUB_TOKEN shared bearer token
  STT_LOG       always append transcripts here (works offline)
"""

import argparse
import json
import logging
import os
import re
import threading
import urllib.request


class OutputHandler:
    def __init__(self, handler_args=None):
        parser = argparse.ArgumentParser()
        parser.add_argument("--hub-url", default=os.getenv("VOICEHUB_URL", "http://127.0.0.1:8790"),
                            help="VoiceHub base URL (prompt callback target)")
        parser.add_argument("--hub-token", nargs="?", const="",
                            default=os.getenv("VOICEHUB_TOKEN", ""))
        parser.add_argument("--log", default=os.getenv("STT_LOG", "/tmp/stt_out.log"),
                            help="always append transcripts here (also when the hub is offline)")
        self.args, _ = parser.parse_known_args(handler_args or [])
        self.base = self.args.hub_url.rstrip("/")
        logging.info(
            f"[voicehub_handler] hub={self.base} log={self.args.log} "
            f"token={'set' if self.args.hub_token else 'off'}"
        )
        self._stop = threading.Event()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    # --- hub I/O -----------------------------------------------------------

    def _post(self, path, payload, timeout=3):
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.args.hub_token:
            headers["Authorization"] = f"Bearer {self.args.hub_token}"
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status

    def _heartbeat_loop(self):
        while not self._stop.wait(30):
            try:
                self._post("/v1/vox/heartbeat", {"source": "vox"}, timeout=3)
            except Exception as e:
                logging.warning(f"[voicehub_handler] heartbeat failed: {e}")

    # --- transcript cleaning (same rules as the old omp_vox_handler) --------

    @staticmethod
    def _clean(text: str) -> str:
        cleaned = text.strip()
        if not cleaned:
            return ""
        cleaned = re.sub(
            r"(\b[\w'-]+\b)(?:[\s,.-]+\1){4,}",
            r"\1 [wiederholt]",
            cleaned,
            flags=re.IGNORECASE,
        )
        hallucination_patterns = [
            r"vielen dank f[üu]r[''’]?s? zuschauen",
            r"vielen dank f[üu]r ihre aufmerksamkeit",
            r"untertitel (?:von|der|im|für|community)",
            r"subtitles by",
            r"amara\.org",
        ]
        for pat in hallucination_patterns:
            if re.search(pat, cleaned, flags=re.IGNORECASE):
                logging.info(f"[voicehub_handler] filtered STT silence hallucination: {cleaned}")
                return ""
        return cleaned

    def _log(self, text):
        try:
            with open(self.args.log, "a", encoding="utf-8") as f:
                f.write(f"{text}\n")
        except Exception as e:
            logging.warning(f"[voicehub_handler] log write failed: {e}")

    # --- vox OutputHandler API ----------------------------------------------

    def send(self, text):
        cleaned = self._clean(text or "")
        if not cleaned:
            return
        self._log(cleaned)
        try:
            status = self._post("/v1/stt", {"text": cleaned, "source": "vox"})
            logging.info(f"[voicehub_handler] sent to hub (HTTP {status}): {cleaned}")
        except Exception as e:
            logging.warning(
                f"[voicehub_handler] hub unreachable ({e}); transcript only logged to {self.args.log}"
            )

    def close(self):
        self._stop.set()
        logging.info("[voicehub_handler] closing handler.")
