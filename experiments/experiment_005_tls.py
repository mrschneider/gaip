#!/usr/bin/env python3
"""GAIP experiment 005 — TLS transport (CSIv2's other half).

Same protocol, same frames: only the socket is wrapped. Proves that the
identity assertion and the encrypted transport compose, and that a plain
client cannot talk to a TLS port.

  A. TLS handshake + CAPABILITY_QUERY answered over the encrypted channel
  B. the same port refuses a plaintext client (no downgrade)
"""

import json, os, socket, ssl, struct, subprocess, sys, tempfile, time

PORT = int(os.environ.get("GAIP_TLS_PORT", "7615"))
ENGINE = "urn:agent:rust:tokio:analytics-engine-v1"
PASSED, FAILED = [], []


def check(label, cond, detail=""):
    (PASSED if cond else FAILED).append(label)
    print(("PASS: " if cond else "FAIL: ") + label + ((" | " + detail) if detail and not cond else ""))


def frame(obj):
    b = json.dumps(obj, separators=(",", ":")).encode()
    return struct.pack(">I", len(b)) + b


def read_frame(s):
    head = s.recv(4)
    if len(head) < 4:
        return None
    n = struct.unpack(">I", head)[0]
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return json.loads(buf)


def main():
    d = tempfile.mkdtemp(prefix="gaip_tls_")
    cert, key = os.path.join(d, "cert.pem"), os.path.join(d, "key.pem")
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", key, "-out", cert, "-days", "1",
         "-subj", "/CN=localhost",
         "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    engine = subprocess.Popen(
        ["./target/debug/gaip-engine", "--port", str(PORT), "--name", ENGINE,
         "--tls-cert", cert, "--tls-key", key, "--idl", "schema/intents.idl"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    time.sleep(0.8)
    if engine.poll() is not None:
        print("FAIL: engine did not start |", engine.stderr.read().decode()[-400:])
        return 1

    try:
        # ---- A. TLS round trip ------------------------------------------
        ctx = ssl.create_default_context(cafile=cert)
        ctx.check_hostname = False
        raw = socket.create_connection(("127.0.0.1", PORT), timeout=8)
        tls = ctx.wrap_socket(raw, server_hostname="localhost")
        check("tls: the handshake completes against the engine's certificate",
              tls.version() is not None, str(tls.version()))
        print(f"   negotiated: {tls.version()} / {tls.cipher()[0]}")

        env = {"protocol_version": "1.0", "message_id": "m_tls", "correlation_id": "task_tls",
               "sender": "urn:agent:python:langgraph:planner-v2", "recipient": ENGINE,
               "message_type": "CAPABILITY_QUERY", "payload": {}}
        tls.sendall(frame({"wire": "REQUEST", "request_id": 1, "contexts": [], "body": env}))
        reply = read_frame(tls)
        payload = ((reply or {}).get("body") or {}).get("payload", {})
        check("tls: a GAIP Request is answered over the encrypted channel",
              (reply or {}).get("wire") == "REPLY" and "capabilities" in payload,
              str(reply)[:160])
        check("tls: the reply carries the same typed payload as over plain TCP",
              payload.get("intents") is not None and payload.get("ior", "").startswith("IOR:"),
              str(payload)[:160])
        tls.close()

        # ---- B. no downgrade --------------------------------------------
        plain = socket.create_connection(("127.0.0.1", PORT), timeout=5)
        plain.sendall(frame({"wire": "LOCATE_REQUEST", "request_id": 2, "target": ENGINE}))
        try:
            data = plain.recv(64)
        except (socket.timeout, ConnectionResetError):
            data = b""
        plain.close()
        # a TLS server answers a plaintext client with either nothing or a
        # TLS alert — never a GAIP reply
        check("tls: a plaintext client gets no GAIP reply (no downgrade)",
              not data.startswith(b"\x00\x00\x00") or b"LOCATE_REPLY" not in data,
              str(data[:40]))
    finally:
        engine.terminate()
        try:
            engine.wait(timeout=5)
        except subprocess.TimeoutExpired:
            engine.kill()

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    print("EXPERIMENT_005:", "PASS" if not FAILED else "FAIL")
    return 0 if not FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
