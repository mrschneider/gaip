#!/usr/bin/env python3
"""GAIP experiment 006 — CORBA-derived robustness.

  1. Failover    a participant's reference survives its crash: the broker
                 answers OBJECT_FORWARD with the endpoint the participant
                 advertised, and the same URN is served there
  2. Persistence the failover registry survives a broker restart
  3. Locks       named locks with leases: exclusion, named refusal,
                 release, and implicit release when the lease expires
  4. Events      run telemetry is an event channel: pull after a cursor,
                 nothing twice, and the stats surface reports counts
"""

import hashlib
import hmac
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

HOST = "127.0.0.1"
SHARED_KEY = b"dev-shared-secret"
ORCH_URN = "urn:agent:python:langgraph:planner-v2"
ENGINE_URN = "urn:agent:rust:tokio:analytics-engine-v1"

PASSED, FAILED = [], []


def check(label, cond, detail=""):
    if cond:
        PASSED.append(label)
        print(f"PASS: {label}")
    else:
        FAILED.append(label)
        print(f"FAIL: {label}" + (f" | {detail}" if detail else ""))


def identity_token(subject="agent:planner-v2", roles=("planner", "executor"), ttl_ms=120_000):
    expires = int(time.time() * 1000) + ttl_ms
    canonical = f"v1|{subject}|{','.join(roles)}||{expires}"
    sig = hmac.new(SHARED_KEY, canonical.encode(), hashlib.sha256).hexdigest()
    return json.dumps({"subject": subject, "roles": list(roles), "on_behalf_of": None,
                       "expires_at_ms": expires, "sig": sig}, separators=(",", ":"))


def envelope(msg_id, corr, mtype, payload, recipient, sender=ORCH_URN):
    return {"protocol_version": "1.0", "message_id": msg_id, "correlation_id": corr,
            "sender": sender, "recipient": recipient, "message_type": mtype,
            "payload": payload}


class Link:
    def __init__(self, port):
        self.sock = socket.create_connection((HOST, port), timeout=10)
        self.buf = bytearray()

    def send(self, msg):
        body = json.dumps(msg, separators=(",", ":")).encode()
        self.sock.sendall(struct.pack(">I", len(body)) + body)

    def recv(self, timeout=6.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(self.buf) >= 4:
                n = struct.unpack(">I", self.buf[:4])[0]
                if len(self.buf) >= 4 + n:
                    body = bytes(self.buf[4:4 + n])
                    del self.buf[:4 + n]
                    return json.loads(body)
            self.sock.settimeout(max(0.05, deadline - time.time()))
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return None
            if not chunk:
                return None
            self.buf.extend(chunk)
        return None

    def request(self, rid, payload, recipient, corr="c1", ctxs=None, sender=None):
        ctxs = ctxs if ctxs is not None else [{"id": 4, "data": identity_token()}]
        self.send({"wire": "REQUEST", "request_id": rid, "contexts": ctxs,
                   "body": envelope(f"m{rid}", corr, "TASK_REQUEST", payload, recipient,
                                    sender=sender or ORCH_URN)})
        return self.recv()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def boot_engine(port, name, log_path=None):
    log = open(log_path, "wb") if log_path else open(os.devnull, "wb")
    p = subprocess.Popen(
        ["./target/debug/gaip-engine", "--host", HOST, "--port", str(port),
         "--name", name, "--key", SHARED_KEY.decode(), "--idl", "schema/intents.idl",
         "--stats-interval", "1"],
        stdout=log, stderr=subprocess.STDOUT)
    time.sleep(0.7)
    return p


def main():
    work = tempfile.mkdtemp(prefix="gaip_exp6_")
    registry = os.path.join(work, "registry.json")
    broker_log = os.path.join(work, "broker.log")
    engine_a_log = os.path.join(work, "engine_a.log")

    PORT_A, PORT_B, PORT_BR = 7616, 7617, 7623
    broker = subprocess.Popen(
        ["./target/debug/gaip-broker", "--port", str(PORT_BR),
         "--urn", "urn:agent:gaip:broker:b1", "--registry", registry],
        stdout=open(broker_log, "wb"), stderr=subprocess.STDOUT)
    time.sleep(0.7)
    if broker.poll() is not None:
        print("FAIL: broker did not start |", open(broker_log).read()[-500:])
        return 1

    try:
        # ---- 1. a participant registers through the broker, advertising
        #         where else it can be reached --------------------------
        engine_a = boot_engine(PORT_A, ENGINE_URN, engine_a_log)
        # a shim links the engine to the broker: the first Request carries
        # the FAILOVER context that advertises the alternate endpoint
        shim = Link(PORT_BR)
        shim.send({"wire": "REQUEST", "request_id": 1, "contexts": [
            {"id": 4, "data": identity_token()},
            {"id": 7, "data": f"{HOST}:{PORT_B}"},
        ], "body": envelope("reg1", "reg", "TASK_REQUEST",
                            {"intent": "PING", "parameters": {}},
                            "urn:agent:gaip:broker:sink", sender=ENGINE_URN)})
        # the sink does not exist; what matters is the registration
        shim.recv(timeout=4)
        time.sleep(0.3)

        # the node dies: the engine AND its broker link
        shim.close()
        engine_a.terminate()
        try:
            engine_a.wait(timeout=5)
        except subprocess.TimeoutExpired:
            engine_a.kill()

        orch = Link(PORT_BR)
        orch.send({"wire": "LOCATE_REQUEST", "request_id": 10, "target": ENGINE_URN})
        loc = orch.recv()
        fwd = (loc or {}).get("status", {}).get("forward", {})
        check("failover: OBJECT_FORWARD carries the advertised alternate",
              (loc or {}).get("status", {}).get("kind") == "OBJECT_FORWARD"
              and fwd.get("urn") == ENGINE_URN,
              str(loc))

        # ---- 2. the registry survives a broker restart -----------------
        broker.terminate()
        try:
            broker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            broker.kill()
        broker = subprocess.Popen(
            ["./target/debug/gaip-broker", "--port", str(PORT_BR),
             "--urn", "urn:agent:gaip:broker:b1", "--registry", registry],
            stdout=open(broker_log, "ab"), stderr=subprocess.STDOUT)
        time.sleep(0.7)
        orch2 = Link(PORT_BR)
        orch2.send({"wire": "LOCATE_REQUEST", "request_id": 11, "target": ENGINE_URN})
        loc2 = orch2.recv()
        check("persistence: the failover registry survives a broker restart",
              (loc2 or {}).get("status", {}).get("kind") == "OBJECT_FORWARD", str(loc2))
        orch2.close()

        # ---- 3. the same URN is served at the alternate endpoint -------
        engine_b_log = os.path.join(work, "engine_b.log")
        engine_b = boot_engine(PORT_B, ENGINE_URN, engine_b_log)
        direct = Link(PORT_B)
        r = direct.request(20, {"intent": "LOCK_RELEASE", "resource": "warmup"}, ENGINE_URN)
        if r is None:
            check("failover: the moved reference serves the same URN", False,
                  "no reply from the alternate endpoint")
        else:
            body = (r.get("body") or {}).get("payload", {})
            check("failover: the moved reference serves the same URN",
                  r.get("status", {}).get("kind") == "NO_EXCEPTION"
                  and body.get("intent") == "LOCK_RELEASE", str(r)[:200])
        direct.close()

        # ---- 4. locks with leases --------------------------------------
        lk = Link(PORT_B)
        a = lk.request(30, {"intent": "LOCK_ACQUIRE", "resource": "dataset-402",
                            "ttl_ms": 60000, "mode": "write"}, ENGINE_URN)
        check("locks: the first holder acquires",
              (a or {}).get("status", {}).get("kind") == "NO_EXCEPTION", str(a)[:160])

        # a second sender is refused, and the refusal names the lock
        OTHER_URN = "urn:agent:python:langgraph:worker-2"
        other = Link(PORT_B)
        other_id = [{"id": 4, "data": identity_token(subject="agent:worker-2")},
                    {"id": 4, "data": identity_token()}]
        b = other.request(31, {"intent": "LOCK_ACQUIRE", "resource": "dataset-402"},
                          ENGINE_URN, ctxs=other_id, corr="c2", sender=OTHER_URN)
        check("locks: a second writer is refused with E_LOCKED",
              (b or {}).get("status", {}).get("kind") == "USER_EXCEPTION"
              and "E_LOCKED" in str((b or {}).get("status")), str(b)[:160])

        # the holder releases; the next acquirer wins
        rel = lk.request(32, {"intent": "LOCK_RELEASE", "resource": "dataset-402"}, ENGINE_URN)
        check("locks: the holder releases",
              (rel or {}).get("status", {}).get("kind") == "NO_EXCEPTION"
              and (rel or {}).get("body", {}).get("payload", {}).get("released") is True,
              str(rel)[:160])
        c = other.request(33, {"intent": "LOCK_ACQUIRE", "resource": "dataset-402"},
                          ENGINE_URN, ctxs=other_id, corr="c2", sender=OTHER_URN)
        check("locks: after release the next acquirer wins",
              (c or {}).get("status", {}).get("kind") == "NO_EXCEPTION", str(c)[:160])

        # a lease expires: the crashed holder cannot deadlock anyone
        other.request(34, {"intent": "LOCK_RELEASE", "resource": "dataset-402"},
                      ENGINE_URN, ctxs=other_id, corr="c2", sender=OTHER_URN)
        d = other.request(35, {"intent": "LOCK_ACQUIRE", "resource": "dataset-402",
                               "ttl_ms": 250}, ENGINE_URN, ctxs=other_id)
        check("locks: a short lease is granted",
              (d or {}).get("status", {}).get("kind") == "NO_EXCEPTION", str(d)[:160])
        time.sleep(0.5)
        e = lk.request(36, {"intent": "LOCK_ACQUIRE", "resource": "dataset-402"}, ENGINE_URN)
        check("locks: an expired lease is released implicitly",
              (e or {}).get("status", {}).get("kind") == "NO_EXCEPTION", str(e)[:160])

        # ---- 5. event channel ------------------------------------------
        # a REAL task publishes task_started; operational intents (locks,
        # pulls) deliberately do not, or every pull would feed itself
        ev_link = Link(PORT_B)
        ev_link.handler = lambda msg: ev_link.send({
            "wire": "REPLY", "request_id": msg["request_id"],
            "status": {"kind": "NO_EXCEPTION"},
            "body": msg["body"] | {"message_id": "ctx_reply",
                                   "sender": msg["body"]["recipient"],
                                   "recipient": msg["body"]["sender"],
                                   "message_type": "TASK_REPLY",
                                   "payload": {"value": "ctx_v7"}}})
        rid = 39
        ev_link.send({"wire": "REQUEST", "request_id": rid, "contexts": [
            {"id": 4, "data": identity_token()},
            {"id": 2, "data": str(int(time.time() * 1000) + 30_000)}],
            "body": envelope("m39", "task_ev", "TASK_REQUEST",
                             {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 402,
                              "horizon_days": 30, "artifacts_ref": []}, ENGINE_URN)})
        deadline = time.time() + 30
        while time.time() < deadline:
            m = ev_link.recv(1.0)
            if m is None:
                break
            if m.get("wire") == "REPLY" and m.get("request_id") == rid \
               and (m.get("body") or {}).get("message_type") != "TASK_STREAM":
                break
        f = ev_link.request(40, {"intent": "EVENT_PULL", "after_seq": 0}, ENGINE_URN)
        evs = (f or {}).get("body", {}).get("payload", {}).get("events", [])
        last = (f or {}).get("body", {}).get("payload", {}).get("last_seq", 0)
        check("events: run telemetry is pullable with a cursor",
              len(evs) > 0 and evs[0]["kind"] == "task_started", str(f)[:200])
        g = ev_link.request(41, {"intent": "EVENT_PULL", "after_seq": last}, ENGINE_URN)
        evs2 = (g or {}).get("body", {}).get("payload", {}).get("events", [])
        check("events: a cursor is not rewound (nothing twice)",
              len(evs2) == 0, str(g)[:200])

        # ---- 6. the stats surface reports measurements ------------------
        time.sleep(1.3)
        stat_lines = [l for l in open(engine_b_log).read().splitlines()
                      if "[engine stats]" in l]
        check("stats: the surface reports counts, not guesses",
              bool(stat_lines) and "requests=" in stat_lines[-1]
              and not stat_lines[-1].endswith("requests=0 replies=0 exceptions=0"),
              stat_lines[-1] if stat_lines else "none")
    finally:
        for p in ("broker", "engine_b"):
            q = locals().get(p)
            if q:
                q.terminate()
                try:
                    q.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    q.kill()

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    print("EXPERIMENT_006:", "PASS" if not FAILED else "FAIL")
    return 0 if not FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
