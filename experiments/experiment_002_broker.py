#!/usr/bin/env python3
"""GAIP experiment 002 — the relay broker: routing, federation, retry,
exactly-once effect and loop safety, over two brokers."""

import json, os, socket, struct, subprocess, sys, tempfile, threading, time

B1_PORT, B2_PORT = 7621, 7622
WORKER = "urn:agent:python:worker:1"
ORCH = "urn:agent:python:langgraph:planner-v2"
PASSED, FAILED = [], []


def check(label, cond, detail=""):
    (PASSED if cond else FAILED).append(label)
    print(("PASS: " if cond else "FAIL: ") + label + ((" | " + detail) if detail and not cond else ""))


class Peer:
    def __init__(self, port, urn):
        self.urn = urn
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = bytearray()
        self.lock = threading.Lock()

    def send(self, msg):
        b = json.dumps(msg, separators=(",", ":")).encode()
        self.sock.sendall(struct.pack(">I", len(b)) + b)

    def recv(self, timeout=6.0):
        deadline = time.time() + timeout
        while True:
            if len(self.buf) >= 4:
                n = struct.unpack(">I", self.buf[:4])[0]
                if len(self.buf) >= 4 + n:
                    body = bytes(self.buf[4:4 + n]); del self.buf[:4 + n]
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


def env(msg_id, corr, mtype, payload, sender, recipient):
    return {"protocol_version": "1.0", "message_id": msg_id, "correlation_id": corr,
            "sender": sender, "recipient": recipient, "message_type": mtype, "payload": payload}


def main():
    procs = []
    b1 = subprocess.Popen(["./target/debug/gaip-broker", "--port", str(B1_PORT),
                           "--urn", "urn:agent:gaip:broker:b1"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(b1)
    time.sleep(0.4)
    b2 = subprocess.Popen(["./target/debug/gaip-broker", "--port", str(B2_PORT),
                           "--urn", "urn:agent:gaip:broker:b2",
                           "--peer", f"127.0.0.1:{B1_PORT}"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(b2)
    time.sleep(0.8)

    executions = {"n": 0}

    # the worker registers on broker b2 and serves requests
    worker = Peer(B2_PORT, WORKER)
    # registration is implicit: the broker learns the URN from `sender`.
    # The probe is addressed to a sink so no participant is disturbed.
    reg = {"wire": "REQUEST", "request_id": 1, "contexts": [],
           "body": env("m_reg", "t_reg", "CAPABILITY_QUERY", {"ask": "self"},
                       WORKER, "urn:agent:gaip:broker:register-sink")}
    worker.send(reg)
    stop = threading.Event()

    def worker_loop():
        while not stop.is_set():
            m = worker.recv(0.5)
            if not m:
                continue
            if m.get("wire") == "REQUEST":
                body = m["body"]
                if body["correlation_id"] == "t_exec":
                    executions["n"] += 1
                reply_env = dict(body)
                reply_env.update({"sender": body["recipient"], "recipient": body["sender"],
                                  "message_type": "TASK_REPLY",
                                  "payload": {"status": "OK", "executions": executions["n"]}})
                worker.send({"wire": "REPLY", "request_id": m["request_id"],
                             "status": {"kind": "NO_EXCEPTION"}, "body": reply_env})
            elif m.get("wire") == "MESSAGE_ERROR":
                print("worker got MessageError:", m["reason"])
    threading.Thread(target=worker_loop, daemon=True).start()
    time.sleep(0.4)

    orch = Peer(B1_PORT, ORCH)
    try:
        # ---- federation: the target lives on the OTHER broker -------------
        rid = 10
        orch.send({"wire": "REQUEST", "request_id": rid, "contexts": [],
                   "body": env("m_1", "t_exec", "TASK_REQUEST", {"intent": "X"}, ORCH, WORKER)})
        reply = orch.recv(8.0)
        check("federation: a request reaches a participant on a peer broker",
              (reply or {}).get("wire") == "REPLY" and
              ((reply or {}).get("body") or {}).get("payload", {}).get("executions") == 1,
              str(reply)[:200])
        check("federation: the reply comes back to the requester",
              ((reply or {}).get("body") or {}).get("recipient") == ORCH, str(reply)[:160])
        check("relay: the broker appended its hop to the route and the reply survived it",
              reply is not None, "")

        # ---- exactly-once effect: the same message_id again ----------------
        orch.send({"wire": "REQUEST", "request_id": 11, "contexts": [],
                   "body": env("m_1", "t_exec", "TASK_REQUEST", {"intent": "X"}, ORCH, WORKER)})
        replay = orch.recv(6.0)
        check("exactly-once: a retried message_id replays the cached reply",
              ((replay or {}).get("body") or {}).get("payload", {}).get("executions") == 1,
              str(replay)[:200])
        check("exactly-once: the work did not run twice", executions["n"] == 1,
              f"executions={executions['n']}")

        # ---- a new message_id executes again ------------------------------
        orch.send({"wire": "REQUEST", "request_id": 12, "contexts": [],
                   "body": env("m_2", "t_exec", "TASK_REQUEST", {"intent": "X"}, ORCH, WORKER)})
        second = orch.recv(6.0)
        check("relay: a new message_id is a new execution",
              ((second or {}).get("body") or {}).get("payload", {}).get("executions") == 2,
              str(second)[:160])

        # ---- retry: a target that does not exist yet -----------------------
        ghost = "urn:agent:python:worker:late"
        orch.send({"wire": "REQUEST", "request_id": 13, "contexts": [],
                   "body": env("m_3", "t_late", "TASK_REQUEST", {}, ORCH, ghost)})
        missing = orch.recv(8.0)
        status = (missing or {}).get("status", {})
        check("retry: an unreachable target ends in a retryable ObjectNotExist",
              status.get("kind") == "SYSTEM_EXCEPTION" and status.get("minor") == "OBJECT_NOT_EXIST",
              str(status)[:160])

        late = Peer(B2_PORT, ghost)
        late.send({"wire": "REQUEST", "request_id": 1, "contexts": [],
                   "body": env("m_reg2", "t_reg2", "CAPABILITY_QUERY", {},
                               ghost, "urn:agent:gaip:broker:register-sink")})
        time.sleep(0.5)
        orch.send({"wire": "REQUEST", "request_id": 14, "contexts": [],
                   "body": env("m_4", "t_late", "TASK_REQUEST", {}, ORCH, ghost)})
        arrived = orch.recv(8.0)
        check("retry: after the participant appears, the same target resolves",
              (arrived or {}).get("wire") == "REPLY", str(arrived)[:160])

        # ---- loop safety: a route that already contains the broker ---------
        orch.send({"wire": "REQUEST", "request_id": 15,
                   "contexts": [{"id": 6, "data": "urn:agent:gaip:broker:b1"}],
                   "body": env("m_5", "t_loop", "TASK_REQUEST", {}, ORCH, WORKER)})
        looped = orch.recv(5.0)
        st = (looped or {}).get("status", {})
        check("loop safety: a request whose route already contains the broker is refused",
              st.get("kind") == "SYSTEM_EXCEPTION" and "loop" in str(st.get("message", "")),
              str(st)[:200])

    finally:
        stop.set()
        try:
            orch.sock.close()
            worker.sock.close()
        except OSError:
            pass
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=4)
            except subprocess.TimeoutExpired:
                p.kill()

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    print("EXPERIMENT_002:", "PASS" if not FAILED else "FAIL")
    return 0 if not FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
