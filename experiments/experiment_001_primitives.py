#!/usr/bin/env python3
"""GAIP experiment 001 — the six primitives over a real binding.

A Python orchestrator (the runtime the protocol exists to abstract) talks
to the Rust reference engine over the framed TCP binding and proves:

  1. CAPABILITY_QUERY        introspection before delegation
  2. TASK_REQUEST → STREAM* → REPLY   the typed happy path
  3. CANCEL_REQUEST          mid-flight abort, acknowledged once
  4. HUMAN_INTERVENT         pause, human decision, resume
  5. closed vocabulary       AGENT_STEP is refused, with the word named
  6. version enforcement     2.0 is refused, 1.x is accepted
  7. addressing              a wrong recipient is refused

Every message is a GAIP envelope; nothing here is framework-specific.
"""

import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

HOST = "127.0.0.1"
PORT = int(os.environ.get("GAIKO_PORT", "7611"))
ENGINE_URN = "urn:agent:rust:tokio:analytics-engine-v1"
ORCH_URN = "urn:agent:python:langgraph:planner-v2"

PASSED, FAILED = [], []


def check(label, cond, detail=""):
    if cond:
        PASSED.append(label)
        print(f"PASS: {label}")
    else:
        FAILED.append(label)
        print(f"FAIL: {label}" + (f" | {detail}" if detail else ""))


class Peer:
    def __init__(self):
        self.sock = socket.create_connection((HOST, PORT), timeout=8)
        self.buf = bytearray()

    def send(self, env):
        body = json.dumps(env).encode()
        self.sock.sendall(struct.pack(">I", len(body)) + body)

    def recv(self, timeout=6.0):
        """Return one frame: an envelope dict, or {'gaip_error': ...}."""
        deadline = time.time() + timeout
        while True:
            if len(self.buf) >= 4:
                n = struct.unpack(">I", self.buf[:4])[0]
                if len(self.buf) >= 4 + n:
                    body = bytes(self.buf[4:4 + n])
                    del self.buf[:4 + n]
                    return json.loads(body)
            if time.time() > deadline:
                return None
            self.sock.settimeout(max(0.05, deadline - time.time()))
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return None
            self.buf.extend(chunk)

    def drain(self, seconds=1.2):
        out = []
        while True:
            got = self.recv(seconds)
            if got is None:
                return out
            out.append(got)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def envelope(msg_id, corr_id, mtype, payload, recipient=ENGINE_URN, sender=ORCH_URN, version="1.0"):
    return {
        "protocol_version": version,
        "message_id": msg_id,
        "correlation_id": corr_id,
        "sender": sender,
        "recipient": recipient,
        "message_type": mtype,
        "payload": payload,
    }


def main():
    log = os.path.join(tempfile.mkdtemp(prefix="gaip_exp_"), "engine.log")
    engine = subprocess.Popen(
        ["./target/debug/gaip-engine", "--host", HOST, "--port", str(PORT), "--name", ENGINE_URN],
        stdout=open(log, "wb"),
        stderr=subprocess.STDOUT,
    )
    time.sleep(0.6)
    if engine.poll() is not None:
        print("FAIL: engine did not start |", open(log).read()[-400:])
        return 1

    peer = Peer()
    try:
        # ---- 1. capability query -----------------------------------------
        peer.send(envelope("msg_1", "task_cap", "CAPABILITY_QUERY", {"ask": "all"}))
        cap = peer.recv()
        caps = (cap or {}).get("payload", {}).get("capabilities", [])
        check(
            "introspection: CAPABILITY_QUERY is answered with a typed TASK_REPLY",
            cap is not None and cap.get("message_type") == "TASK_REPLY" and "INVENTORY_OPTIMIZATION" in caps,
            str(cap)[:160],
        )
        check(
            "introspection: the answer is correlated to the query",
            cap is not None and cap.get("correlation_id") == "task_cap",
            str((cap or {}).get("correlation_id")),
        )

        # ---- 2. the happy path -------------------------------------------
        task = "task_happy"
        peer.send(envelope(
            "msg_10", task, "TASK_REQUEST",
            {"intent": "INVENTORY_OPTIMIZATION",
             "parameters": {"facility_id": 402, "horizon_days": 30},
             "artifacts_ref": ["s3://enterprise-data/warehouse_402.parquet"]},
        ))
        frames = peer.drain(6.0)
        streams = [f for f in frames if f.get("message_type") == "TASK_STREAM"]
        replies = [f for f in frames if f.get("message_type") == "TASK_REPLY"]
        check(
            "lifecycle: TASK_REQUEST produces three TASK_STREAM frames",
            [s["payload"]["progress"] for s in streams] == [25, 50, 75],
            str([s.get("payload", {}).get("progress") for s in streams]),
        )
        check(
            "lifecycle: then exactly one TASK_REPLY",
            len(replies) == 1,
            str(len(replies)),
        )
        check(
            "typing: every frame carries the same correlation_id",
            all(f.get("correlation_id") == task for f in frames) and frames,
        )
        reply = replies[0] if replies else {}
        check(
            "typing: the reply carries the typed result, not prose",
            reply.get("payload", {}).get("status") == "OK"
            and reply.get("payload", {}).get("result", {}).get("projected_saving_pct") == 12.4,
            str(reply.get("payload"))[:160],
        )
        check(
            "addressing: the reply is sent back to the requester URN",
            reply.get("sender") == ENGINE_URN and reply.get("recipient") == ORCH_URN,
            f"{reply.get('sender')} -> {reply.get('recipient')}",
        )

        # ---- 3. cancellation ----------------------------------------------
        task2 = "task_cancel"
        peer.send(envelope("msg_20", task2, "TASK_REQUEST", {"intent": "INVENTORY_OPTIMIZATION"}))
        first = peer.recv(4.0)
        check(
            "cancel: the task starts streaming before we abort it",
            first is not None and first.get("message_type") == "TASK_STREAM",
            str(first)[:120],
        )
        peer.send(envelope("msg_21", task2, "CANCEL_REQUEST", {"reason": "user aborted"}))
        ack = peer.recv(4.0)
        check(
            "cancel: CANCEL_REQUEST is acknowledged with status CANCELLED",
            ack is not None
            and ack.get("message_type") == "TASK_REPLY"
            and ack.get("payload", {}).get("status") == "CANCELLED",
            str(ack)[:160],
        )
        after = [f for f in peer.drain(1.5) if f.get("correlation_id") == task2]
        check(
            "cancel: no further frames arrive for the aborted task",
            all(f.get("message_type") != "TASK_STREAM" for f in after),
            str([f.get("message_type") for f in after]),
        )

        # ---- 4. human in the loop ------------------------------------------
        task3 = "task_human"
        peer.send(envelope(
            "msg_30", task3, "TASK_REQUEST",
            {"intent": "INVENTORY_OPTIMIZATION", "requires_human": True},
        ))
        pause = peer.recv(4.0)
        check(
            "human: a risky task raises HUMAN_INTERVENT before computing",
            pause is not None and pause.get("message_type") == "HUMAN_INTERVENT",
            str(pause)[:160],
        )
        check(
            "human: the intervention names what is being approved",
            (pause or {}).get("payload", {}).get("proposed_action") is not None,
            str((pause or {}).get("payload"))[:120],
        )
        nothing = [f for f in peer.drain(0.8) if f.get("correlation_id") == task3]
        check("human: the task does not run while it waits", not nothing,
              str([f.get("message_type") for f in nothing]))

        peer.send(envelope(
            "msg_31", task3, "TASK_REPLY", {"decision": "APPROVE"}, sender=ORCH_URN,
        ))
        resumed = peer.drain(6.0)
        resumed_streams = [f for f in resumed if f.get("message_type") == "TASK_STREAM"]
        resumed_reply = [f for f in resumed if f.get("message_type") == "TASK_REPLY"]
        check(
            "human: the decision resumes the work (streams continue)",
            len(resumed_streams) == 3,
            str(len(resumed_streams)),
        )
        check(
            "human: and the task then completes normally",
            len(resumed_reply) == 1 and resumed_reply[0]["payload"].get("status") == "OK",
            str([f.get("payload", {}).get("status") for f in resumed_reply]),
        )

        # ---- 5. closed vocabulary -------------------------------------------
        peer.send(envelope("msg_40", "task_drift", "AGENT_STEP", {"step": "planning"}))
        drift = peer.recv(3.0)
        text = json.dumps(drift or {})
        check(
            "drift: a framework-specific verb is refused, naming the word",
            "gaip_error" in text and "AGENT_STEP" in text,
            text[:200],
        )
        check(
            "drift: the refusal lists the six legal primitives",
            "TASK_REQUEST" in text and "HUMAN_INTERVENT" in text,
            text[:200],
        )

        # ---- 6. version enforcement ------------------------------------------
        peer.send(envelope("msg_41", "task_v2", "TASK_REQUEST", {"intent": "X"}, version="2.0"))
        vres = peer.recv(3.0)
        check(
            "version: protocol_version 2.0 is refused",
            "gaip_error" in json.dumps(vres or {}) and "2.0" in json.dumps(vres or {}),
            json.dumps(vres or {})[:200],
        )
        peer.send(envelope("msg_42", "task_v1x", "TASK_REQUEST", {"intent": "X"}, version="1.4"))
        vok = peer.drain(5.0)
        check(
            "version: a 1.x minor version is accepted (additive by contract)",
            any(f.get("correlation_id") == "task_v1x" for f in vok),
            str([f.get("message_type") for f in vok]),
        )

        # ---- 7. addressing ---------------------------------------------------
        peer.send(envelope(
            "msg_43", "task_wrong", "TASK_REQUEST", {"intent": "X"},
            recipient="urn:agent:java:spring:legacy-connector",
        ))
        wrong = peer.recv(3.0)
        check(
            "addressing: a message addressed to another participant is refused",
            "gaip_error" in json.dumps(wrong or {})
            and "legacy-connector" in json.dumps(wrong or {}),
            json.dumps(wrong or {})[:200],
        )

    finally:
        peer.close()
        engine.terminate()
        try:
            engine.wait(timeout=5)
        except subprocess.TimeoutExpired:
            engine.kill()

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        print("EXPERIMENT_001: FAIL")
        return 1
    print("EXPERIMENT_001: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
