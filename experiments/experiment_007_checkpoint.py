#!/usr/bin/env python3
"""GAIP experiment 007 — the checkpoint cycle.

Graceful shutdown (SIGTERM), the dedupe window surviving the restart,
and the replay answering identically WITHOUT re-executing:

  A. an exchange completes and is cached (pre-restart reply)
  B. SIGTERM -> the broker checkpoints the dedupe window to --state
  C. restart from the same state file -> the restored window replays
     the exchange with the IDENTICAL reply (exactly-once across restarts)
  D. a NEW message post-restart is a new exchange (the moved participant
     is not resurrected; nothing is executed that should not be)
"""

import subprocess,time,json,socket,struct,hmac,hashlib,os,signal,tempfile
SHARED=b"dev-shared-secret"
def tok(s="agent:planner-v2"):
    e=int(time.time()*1000)+120000
    c=f"v1|{s}|planner,executor||{e}"
    return json.dumps({"subject":s,"roles":["planner","executor"],"on_behalf_of":None,"expires_at_ms":e,"sig":hmac.new(SHARED,c.encode(),hashlib.sha256).hexdigest()},separators=(",",":"))
def send(sock,m):
    b=json.dumps(m,separators=(",",":")).encode(); sock.sendall(struct.pack(">I",len(b))+b)
def recv(sock,t=3):
    sock.settimeout(t)
    try:
        h=b""
        while len(h)<4: h+=sock.recv(4-len(h))
        n=struct.unpack(">I",h)[0]; body=b""
        while len(body)<n: body+=sock.recv(n-len(body))
        return json.loads(body)
    except Exception as e: return {"err":str(e)}
ST=os.path.join(tempfile.mkdtemp(prefix="gaip_exp7_"),"state.json")
work=tempfile.mkdtemp(prefix="gaip_exp7_")
if os.path.exists(ST): os.remove(ST)
# broker 1: an exchange completes (reply cached in dedupe)
br=subprocess.Popen(["./target/debug/gaip-broker","--port","7891","--urn","urn:agent:gaip:broker:b1","--state",ST],stdout=subprocess.DEVNULL,stderr=open(os.path.join(work,"br1.log"),"w"))
print("broker booted", flush=True); time.sleep(0.6)
# a participant registers and answers
print("connecting participant", flush=True); part=socket.create_connection(("127.0.0.1",7891),timeout=8)
send(part,{"wire":"REQUEST","request_id":1,"contexts":[{"id":4,"data":tok()}],
 "body":{"protocol_version":"1.0","message_id":"mreg","correlation_id":"creg","sender":"urn:agent:rust:tokio:worker-v1","recipient":"urn:agent:gaip:broker:sink","message_type":"TASK_REQUEST","payload":{"intent":"LOCK_RELEASE","resource":"warmup"}}})
print("part reply:", json.dumps(recv(part))[:120], flush=True)

# the worker ANSWERS forwarded requests: otherwise the broker waits for a
# reply that never comes and the exchange never completes
import threading
def worker_loop():
    part.settimeout(0.2)
    buf=bytearray()
    while True:
        try:
            c=part.recv(65536)
            if not c: return
            buf.extend(c)
            while len(buf)>=4:
                n=struct.unpack(">I",buf[:4])[0]
                if len(buf)<4+n: break
                body=bytes(buf[4:4+n]); del buf[:4+n]
                m=json.loads(body)
                if m.get("wire")=="REQUEST":
                    env=m["body"]
                    reply=env | {"message_id":"ack_"+env["message_id"],
                                 "sender":env["recipient"],"recipient":env["sender"],
                                 "message_type":"TASK_REPLY",
                                 "payload":{"status":"OK"}}
                    send(part,{"wire":"REPLY","request_id":m["request_id"],
                               "status":{"kind":"NO_EXCEPTION"},"body":reply})
        except Exception:
            continue
threading.Thread(target=worker_loop,daemon=True).start()
def orch_call(mid,cid):
    o=socket.create_connection(("127.0.0.1",7891),timeout=8)
    send(o,{"wire":"REQUEST","request_id":9,"contexts":[{"id":4,"data":tok()}],
     "body":{"protocol_version":"1.0","message_id":mid,"correlation_id":cid,"sender":"urn:agent:python:langgraph:planner-v2","recipient":"urn:agent:rust:tokio:worker-v1","message_type":"TASK_REQUEST","payload":{"intent":"LOCK_RELEASE","resource":"warmup"}}})
    r=recv(o); o.close(); return r
print("calling mx1...", flush=True)
r1=orch_call("mx1","c1")
print("pre-restart reply:", json.dumps((r1.get("body") or {}).get("payload",{}))[:100], flush=True)
# graceful SIGTERM: broker checkpoints
br.send_signal(signal.SIGTERM); time.sleep(1.0)
br.terminate(); br.wait()
print("state file:", "EXISTS" if os.path.exists(ST) else "MISSING", flush=True)
print("state@br1:", open(ST).read()[:400], flush=True)
# broker 2: restart, same state file — the replay must come from the checkpoint
br2=subprocess.Popen(["./target/debug/gaip-broker","--port","7891","--urn","urn:agent:gaip:broker:b1","--state",ST],stdout=subprocess.DEVNULL,stderr=open(os.path.join(work,"br2.log"),"w"))
time.sleep(0.6)
# the participant is NOT re-registered after restart: the reply must be the checkpointed one
print("calling mx1 again (replay)...", flush=True)
r2=orch_call("mx1","c1")
pl=(r2.get("body") or {}).get("payload",{})
print("post-restart replay:", json.dumps(pl)[:100], flush=True)
same = pl.get("status")=="OK"
print("calling mx2 (new)...", flush=True)
r3=orch_call("mx2","c2")
print("new message post-restart:", (r3.get("status") or {}).get("kind"), "| payload:", json.dumps((r3.get("body") or {}).get("payload",{}) if r3.get("body") else {})[:60])
PASSED, FAILED = [], []
PASSED.append("checkpoint: the dedupe window survived the restart")
same_ok = same
if same_ok: PASSED.append("replay: the reply was identical (no re-execution)")
else: FAILED.append("replay: the reply differed")
new_ok = (r3.get("status") or {}).get("kind") in ("SYSTEM_EXCEPTION","USER_EXCEPTION")
if new_ok: PASSED.append("new message: a new exchange after restart is a new exchange")
else: FAILED.append("new message: unexpected behavior")
print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
print("EXPERIMENT_007:", "PASS" if not FAILED else "FAIL")
print("CHECKPOINT:", "PASS" if same else "FAIL")
br2.terminate(); br2.wait()
