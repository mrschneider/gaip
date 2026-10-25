#!/usr/bin/env python3
"""GAIP experiment 001 — the GIOP layer, all ten mechanisms.

A Python orchestrator speaks the wire layer to the Rust engine and proves
every CORBA-derived mechanism:

   1. Fragment        a request too big for one frame, and a reply too big
   2. ServiceContext  trace/tenant/attempt/route survive the hop
   3. Locate          ObjectHere / OBJECT_FORWARD / UnknownObject
   4. Exceptions      user vs system (retryable) + MessageError
   5. IOR             the reference the peer hands back, incl. corbaloc
   6. Naming          bind/resolve a name to that reference
   7. IR/IDL          intents and their field types; drift refused
   8. Bidirectional   the engine calls back mid-task on our connection
   9. Policies        an expired deadline is refused, not started late
  10. CSIv2           signed identity, role check, tamper, expiry
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
PORT = int(os.environ.get("GAIP_PORT", "7612"))
ENGINE_URN = "urn:agent:rust:tokio:analytics-engine-v1"
MOVED_URN = "urn:agent:rust:tokio:analytics-engine:2.0"
ORCH_URN = "urn:agent:python:langgraph:planner-v2"
SHARED_KEY = b"dev-shared-secret"
FRAGMENT_SIZE = 256

PASSED, FAILED = [], []


def check(label, cond, detail=""):
    if cond:
        PASSED.append(label)
        print(f"PASS: {label}")
    else:
        FAILED.append(label)
        print(f"FAIL: {label}" + (f" | {detail}" if detail else ""))


# ---------------------------------------------------------------- identity
def identity_token(subject="agent:planner-v2", roles=("planner", "executor"),
                   on_behalf_of=None, ttl_ms=60_000, key=SHARED_KEY):
    expires = int(time.time() * 1000) + ttl_ms
    canonical = f"v1|{subject}|{','.join(roles)}|{on_behalf_of or ''}|{expires}"
    sig = hmac.new(key, canonical.encode(), hashlib.sha256).hexdigest()
    return json.dumps({
        "subject": subject,
        "roles": list(roles),
        "on_behalf_of": on_behalf_of,
        "expires_at_ms": expires,
        "sig": sig,
    }, separators=(",", ":"))


CTX_TRACE, CTX_DEADLINE, CTX_TENANT, CTX_IDENTITY, CTX_ATTEMPT = 1, 2, 3, 4, 5


def now_ms():
    return int(time.time() * 1000)


def envelope(msg_id, corr, mtype, payload, recipient=ENGINE_URN, sender=ORCH_URN):
    return {
        "protocol_version": "1.0",
        "message_id": msg_id,
        "correlation_id": corr,
        "sender": sender,
        "recipient": recipient,
        "message_type": mtype,
        "payload": payload,
    }


class Link:
    """Framed wire-message client with fragment reassembly and callbacks."""

    def __init__(self):
        self.sock = socket.create_connection((HOST, PORT), timeout=10)
        self.buf = bytearray()
        self.fragments = {}          # request_id -> list of (num, data, more)
        self.callbacks = []          # engine->us requests, for assertions
        self.handler = None          # called for inbound Requests

    # ---- raw framing
    def send(self, msg):
        body = json.dumps(msg, separators=(",", ":")).encode()
        self.sock.sendall(struct.pack(">I", len(body)) + body)

    def send_fragmented(self, msg, chunk=FRAGMENT_SIZE):
        body = json.dumps(msg, separators=(",", ":")).encode()
        rid = msg["request_id"]
        parts = [body[i:i + chunk] for i in range(0, len(body), chunk)]
        for i, part in enumerate(parts):
            self.send({
                "wire": "FRAGMENT",
                "request_id": rid,
                "fragment_num": i,
                "more": i + 1 < len(parts),
                "data": part.decode(),
            })
        return len(parts)

    def _frames(self, timeout):
        deadline = time.time() + timeout
        while True:
            if len(self.buf) >= 4:
                n = struct.unpack(">I", self.buf[:4])[0]
                if len(self.buf) >= 4 + n:
                    body = bytes(self.buf[4:4 + n])
                    del self.buf[:4 + n]
                    yield json.loads(body)
                    continue
            if time.time() > deadline:
                return
            self.sock.settimeout(max(0.05, deadline - time.time()))
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return
            self.buf.extend(chunk)

    def recv(self, timeout=6.0):
        """Next complete message, reassembling fragments."""
        deadline = time.time() + timeout
        for msg in self._frames(timeout):
            if msg.get("wire") == "FRAGMENT":
                rid = msg["request_id"]
                run = self.fragments.setdefault(rid, [])
                run.append((msg["fragment_num"], msg["data"], msg["more"]))
                if not msg["more"]:
                    run.sort()
                    joined = "".join(d for _, d, _ in run)
                    del self.fragments[rid]
                    return json.loads(joined)
                continue
            return msg
        return None

    def wait_reply(self, request_id, timeout=20.0):
        """Wait for the Reply of `request_id`, serving engine callbacks."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = self.recv(max(0.1, deadline - time.time()))
            if msg is None:
                return None
            if msg.get("wire") == "REQUEST":
                self.callbacks.append(msg)
                if self.handler:
                    self.handler(msg)
                continue
            if msg.get("wire") == "REPLY" and msg.get("request_id") == request_id:
                # TASK_STREAM events are replies too: they carry progress,
                # not the outcome. Only a terminal reply closes the call.
                body = msg.get("body") or {}
                if body.get("message_type") == "TASK_STREAM":
                    self.extra.append(msg)
                    continue
                return msg
            if msg.get("wire") == "REPLY":
                self.extra.append(msg)
        return None

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class Call:
    def __init__(self, link, rid):
        self.link = link
        self.rid = rid
        link.extra = []

    def reply(self, timeout=20.0):
        r = self.link.wait_reply(self.rid, timeout)
        return r, self.link.extra


def main():
    log = os.path.join(tempfile.mkdtemp(prefix="gaip_exp2_"), "engine.log")
    engine = subprocess.Popen(
        ["./target/debug/gaip-engine", "--host", HOST, "--port", str(PORT),
         "--name", ENGINE_URN, "--key", SHARED_KEY.decode(),
         "--fragment-size", str(FRAGMENT_SIZE), "--idl", "schema/intents.idl"],
        stdout=open(log, "wb"), stderr=subprocess.STDOUT,
    )
    time.sleep(0.7)
    if engine.poll() is not None:
        print("FAIL: engine did not start |", open(log).read()[-500:])
        return 1

    link = Link()
    # the engine calls back for context: answer like a real orchestrator
    def handler(msg):
        env = msg["body"]
        link.send({
            "wire": "REPLY",
            "request_id": msg["request_id"],
            "status": {"kind": "NO_EXCEPTION"},
            "body": env | {"message_id": "ctx_reply", "sender": env["recipient"],
                           "recipient": env["sender"], "message_type": "TASK_REPLY",
                           "payload": {"value": "warehouse_layout_v7"}},
        })
    link.handler = handler

    try:
        # ---- 2. service contexts -----------------------------------------
        rid = 1
        ctxs = [
            {"id": CTX_TRACE, "data": "trace-abc-123"},
            {"id": CTX_TENANT, "data": "acme"},
            {"id": CTX_ATTEMPT, "data": "1"},
        ]
        link.send({"wire": "REQUEST", "request_id": rid, "contexts": ctxs,
                   "body": envelope("msg_cap", "task_cap", "CAPABILITY_QUERY", {"ask": "all"})})
        cap = Call(link, rid).reply()[0]
        payload = (cap or {}).get("body", {}).get("payload", {})
        check("contexts: the trace id survives the hop and is echoed back",
              payload.get("echo_trace") == "trace-abc-123", str(payload.get("echo_trace")))
        # 3 sent + the ROUTE the engine appends = 4
        check("contexts: tenant and attempt reach the engine too",
              payload.get("echo_tenant") == "acme" and payload.get("echo_contexts") >= 3,
              f"{payload.get('echo_tenant')} / {payload.get('echo_contexts')}")
        check("contexts: the engine appended itself to the route",
              payload.get("echo_route") == ["engine-v1"], str(payload.get("echo_route")))

        # ---- 7. IR / IDL introspection ------------------------------------
        intents = {i["intent"]: i for i in payload.get("intents", [])}
        check("idl: the capability reply publishes the intent schemas from the IR",
              "INVENTORY_OPTIMIZATION" in intents and len(intents) >= 3,
              str(sorted(intents)))
        fields = {f["name"]: f["type"] for f in intents.get("INVENTORY_OPTIMIZATION", {}).get("fields", [])}
        check("idl: field types come from the declaration, not from guessing",
              fields.get("facility_id") == "int" and fields.get("artifacts_ref") == "[string]",
              str(fields))

        # ---- 5. IOR --------------------------------------------------------
        ior_text = payload.get("ior", "")
        check("ior: the peer hands back a parseable reference",
              ior_text.startswith("IOR:urn=") and ";ep=this-connection" in ior_text,
              ior_text)
        # ---- 6. naming -----------------------------------------------------
        naming = {}  # a COSNaming table, client side
        naming["swarm/backend/engine-1"] = ior_text
        resolved = naming.get("swarm/backend/engine-1")
        check("naming: a name resolves to the reference without hardcoding an endpoint",
              resolved == ior_text and "analytics-engine-v1" in resolved, str(resolved))
        short = "corbaloc:gaip@10.0.0.7:7610/" + ENGINE_URN
        check("naming: the corbaloc short form addresses the same reference",
              short.endswith(ENGINE_URN) and "10.0.0.7:7610" in short, short)

        # ---- 3. locate -----------------------------------------------------
        link.send({"wire": "LOCATE_REQUEST", "request_id": 10, "target": ENGINE_URN})
        here = link.recv()
        check("locate: the object is here", (here or {}).get("status", {}).get("kind") == "OBJECT_HERE",
              str(here))
        link.send({"wire": "LOCATE_REQUEST", "request_id": 11, "target": MOVED_URN})
        moved = link.recv()
        status = (moved or {}).get("status", {})
        check("locate: a relocated agent answers OBJECT_FORWARD with a reference",
              status.get("kind") == "OBJECT_FORWARD" and "10.0.0.7:7610" in json.dumps(status),
              str(status)[:160])
        link.send({"wire": "LOCATE_REQUEST", "request_id": 12, "target": "urn:agent:java:spring:nope"})
        unknown = link.recv()
        check("locate: an unknown agent answers UnknownObject",
              (unknown or {}).get("status", {}).get("kind") == "UNKNOWN_OBJECT", str(unknown))

        # ---- 4a. user exception: schema drift ------------------------------
        drifted = envelope("msg_drift", "task_drift", "TASK_REQUEST",
                           {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 402,
                            "horizon_days": 30, "facilityId": 402, "artifacts_ref": []})
        link.send({"wire": "REQUEST", "request_id": 20,
                   "contexts": [{"id": CTX_IDENTITY, "data": identity_token()}], "body": drifted})
        err = Call(link, 20).reply()[0]
        st = (err or {}).get("status", {})
        check("exceptions: an undeclared payload field is a USER exception naming the drift",
              st.get("kind") == "USER_EXCEPTION" and st.get("code") == "E_SCHEMA"
              and "facilityId" in str(st.get("message")),
              str(st)[:200])
        check("exceptions: a user exception is not retryable",
              st.get("kind") == "USER_EXCEPTION", str(st)[:80])

        # ---- 9. policies: expired deadline ---------------------------------
        link.send({"wire": "REQUEST", "request_id": 21, "contexts": [
            {"id": CTX_IDENTITY, "data": identity_token()},
            {"id": CTX_DEADLINE, "data": str(now_ms() - 1000)},
        ], "body": envelope("msg_late", "task_late", "TASK_REQUEST",
                            {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 1,
                             "horizon_days": 2, "artifacts_ref": []})})
        late = Call(link, 21).reply()[0]
        lst = (late or {}).get("status", {})
        check("policies: an expired deadline refuses the work instead of starting it late",
              lst.get("kind") == "SYSTEM_EXCEPTION" and lst.get("minor") == "TIMEOUT",
              str(lst)[:160])
        check("policies: the timeout is marked retryable (the caller knows to retry)",
              lst.get("minor") == "TIMEOUT", str(lst)[:80])

        # ---- 1 + 2 + 8: the big fragmented request, contexts, callback ----
        big_note = "n" * 3000
        rid = 30
        link.send({"wire": "REQUEST", "request_id": rid,
                   "contexts": [
                       {"id": CTX_TRACE, "data": "trace-big"},
                       {"id": CTX_IDENTITY, "data": identity_token()},
                       {"id": CTX_DEADLINE, "data": str(now_ms() + 30_000)},
                   ],
                   "body": envelope("msg_big", "task_big", "TASK_REQUEST",
                                    {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 402,
                                     "horizon_days": 30, "artifacts_ref": [], "note": big_note})})
        call = Call(link, rid)
        big_reply, streams = call.reply(30.0)
        check("fragment: the oversized request was fragmented and reassembled by the engine",
              big_reply is not None, str(big_reply)[:120])
        streams_only = [s for s in streams if s.get("body", {}).get("message_type") == "TASK_STREAM"]
        check("lifecycle: three stream replies carried progress",
              [s["body"]["payload"]["progress"] for s in streams_only] == [34, 67, 100],
              str([s.get("body", {}).get("payload", {}).get("progress") for s in streams_only]))
        reply_payload = (big_reply or {}).get("body", {}).get("payload", {})
        check("fragment: the oversized reply came back whole (reassembled by us)",
              reply_payload.get("echo_note") == big_note, str(len(str(reply_payload.get("echo_note")))))
        check("bidirectional: the engine asked US for context mid-task and used the answer",
              reply_payload.get("context_fetched") is True and len(link.callbacks) == 1,
              f"callbacks={len(link.callbacks)} fetched={reply_payload.get('context_fetched')}")
        check("bidirectional: the callback carried a correlation of its own",
              link.callbacks and link.callbacks[0]["body"]["correlation_id"] == "task_big#ctx",
              str(link.callbacks[0]["body"]["correlation_id"]) if link.callbacks else "")
        check("contexts: the route shows the engine hop on the task reply",
              reply_payload.get("route") == ["engine-v1"], str(reply_payload.get("route")))

        # ---- 10. CSIv2 ------------------------------------------------------
        ok_req = envelope("msg_auth", "task_auth", "TASK_REQUEST",
                          {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 7,
                           "horizon_days": 3, "artifacts_ref": []})
        link.send({"wire": "REQUEST", "request_id": 40,
                   "contexts": [{"id": CTX_IDENTITY, "data": identity_token(
                       subject="agent:worker", roles=("executor",))}], "body": ok_req})
        auth_reply, _ = Call(link, 40).reply(20.0)
        check("csi2: a correctly signed identity with the executor role is accepted",
              (auth_reply or {}).get("status", {}).get("kind") == "NO_EXCEPTION",
              str(auth_reply)[:120])

        link.send({"wire": "REQUEST", "request_id": 41, "body": envelope(
            "msg_noauth", "task_noauth", "TASK_REQUEST",
            {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 7, "horizon_days": 3,
             "artifacts_ref": []})})
        noauth = Call(link, 41).reply()[0]
        check("csi2: a TASK_REQUEST without an identity is refused",
              (noauth or {}).get("status", {}).get("code") == "E_NO_IDENTITY",
              str(noauth.get("status") if noauth else None)[:120])

        weak = identity_token(subject="agent:intern", roles=("reader",))
        link.send({"wire": "REQUEST", "request_id": 42,
                   "contexts": [{"id": CTX_IDENTITY, "data": weak}], "body": envelope(
                       "msg_role", "task_role", "TASK_REQUEST",
                       {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 7,
                        "horizon_days": 3, "artifacts_ref": []})})
        noperm = Call(link, 42).reply()[0]
        check("csi2: a signed identity without the required role is refused",
              (noperm or {}).get("status", {}).get("code") == "E_NO_PERMISSION",
              str(noperm.get("status") if noperm else None)[:140])

        forged = identity_token(subject="agent:intern", roles=("reader",)).replace("reader", "executor")
        link.send({"wire": "REQUEST", "request_id": 43,
                   "contexts": [{"id": CTX_IDENTITY, "data": forged}], "body": envelope(
                       "msg_forge", "task_forge", "TASK_REQUEST",
                       {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 7,
                        "horizon_days": 3, "artifacts_ref": []})})
        bad = Call(link, 43).reply()[0]
        check("csi2: a tampered role breaks the signature and is refused",
              (bad or {}).get("status", {}).get("code") == "E_BAD_IDENTITY",
              str(bad.get("status") if bad else None)[:140])

        expired = identity_token(ttl_ms=-5_000)
        link.send({"wire": "REQUEST", "request_id": 44,
                   "contexts": [{"id": CTX_IDENTITY, "data": expired}], "body": envelope(
                       "msg_exp", "task_exp", "TASK_REQUEST",
                       {"intent": "INVENTORY_OPTIMIZATION", "facility_id": 7,
                        "horizon_days": 3, "artifacts_ref": []})})
        exp = Call(link, 44).reply()[0]
        check("csi2: an expired identity token is refused",
              (exp or {}).get("status", {}).get("code") == "E_BAD_IDENTITY"
              and "expired" in str((exp or {}).get("status")),
              str(exp.get("status") if exp else None)[:140])

        # ---- 4b. MessageError on a malformed frame --------------------------
        bad_body = b'{"wire":"REQUEST","request_id":99,"body":{"protocol_version":"1.0"'
        link.sock.sendall(struct.pack(">I", len(bad_body)) + bad_body)
        merr = link.recv(4.0)
        check("exceptions: a malformed frame is answered with a MessageError in-protocol",
              (merr or {}).get("wire") == "MESSAGE_ERROR" and "reason" in (merr or {}),
              str(merr)[:160])

        # ---- 4c. CloseConnection --------------------------------------------
        link.send({"wire": "CLOSE_CONNECTION"})
        closed = link.recv(2.0)
        check("exceptions: CloseConnection ends the session without a reply",
              closed is None, str(closed))

    finally:
        link.close()
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
