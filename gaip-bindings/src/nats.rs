//! NATS binding: GAIP over a subject bus.
//!
//! NATS is the asynchronous arm of the transport strategy: a task can be
//! delegated to a subject instead of a specific process, so the
//! orchestrator does not need to know who is listening. The wire message
//! is the payload — nothing about the contract changes.
//!
//! Subject convention (documented, not enforced by the bus):
//!
//! ```text
//! gaip.<primitive>.<correlation>     e.g. gaip.task_request.task_123
//! gaip.reply.<correlation>
//! ```
//!
//! The client here speaks the NATS text protocol directly (`INFO`,
//! `CONNECT`, `SUB`, `PUB`, `MSG`, `PING`/`PONG`) so the binding carries
//! no C dependency and the exchange is inspectable in a capture.

use gaip_core::codec::{decode_wire, encode_wire};
use gaip_core::{GaipError, WireMessage};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A GAIP participant on a NATS subject bus.
pub struct NatsBinding {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    info: String,
}

impl NatsBinding {
    /// Connect and complete the NATS handshake (`INFO` -> `CONNECT` ->
    /// `PING`/`PONG`), so a returned binding is known-good.
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let writer = stream.try_clone()?;
        let mut reader = BufReader::new(stream);

        let mut info = String::new();
        reader.read_line(&mut info)?;
        if !info.starts_with("INFO ") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected INFO from the bus, got: {}", info.trim()),
            ));
        }
        let mut b = NatsBinding {
            reader,
            writer,
            info: info.trim().to_string(),
        };
        b.writer.write_all(
            b"CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"gaip\",\"version\":\"0.3.0\",\"protocol\":1}\r\n",
        )?;
        b.writer.write_all(b"PING\r\n")?;
        b.writer.flush()?;
        // PONG confirms the connection is authenticated and usable
        let mut line = String::new();
        b.reader.read_line(&mut line)?;
        if line.trim() != "PONG" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected PONG, got: {}", line.trim()),
            ));
        }
        Ok(b)
    }

    /// The server's `INFO` line, kept for diagnostics (cluster name,
    /// max payload, auth requirements).
    pub fn server_info(&self) -> &str {
        &self.info
    }

    pub fn subscribe(&mut self, subject: &str) -> std::io::Result<()> {
        self.subscribe_with_sid(subject, "1")
    }

    pub fn subscribe_with_sid(&mut self, subject: &str, sid: &str) -> std::io::Result<()> {
        self.writer
            .write_all(format!("SUB {subject} {sid}\r\n").as_bytes())?;
        self.writer.flush()?;
        // flush marker: guarantees the SUB reached the server before a
        // publisher on another connection fires
        self.writer.write_all(b"PING\r\n")?;
        self.writer.flush()?;
        self.expect_pong()
    }

    fn expect_pong(&mut self) -> std::io::Result<()> {
        let mut line = String::new();
        loop {
            line.clear();
            self.reader.read_line(&mut line)?;
            match line.trim() {
                "PONG" => return Ok(()),
                "" => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "bus closed during handshake",
                    ))
                }
                other if other.starts_with("PING") => {
                    self.writer.write_all(b"PONG\r\n")?;
                    self.writer.flush()?;
                }
                _ => continue, // +OK, MSG arriving early, etc.
            }
        }
    }

    /// Publish a wire message on a subject. Returns the payload size.
    pub fn publish(&mut self, subject: &str, msg: &WireMessage) -> Result<usize, GaipError> {
        let payload = encode_wire(msg)?;
        self.writer
            .write_all(format!("PUB {subject} {}\r\n", payload.len()).as_bytes())
            .map_err(|e| GaipError::Json(e.to_string()))?;
        self.writer
            .write_all(&payload)
            .map_err(|e| GaipError::Json(e.to_string()))?;
        self.writer
            .write_all(b"\r\n")
            .map_err(|e| GaipError::Json(e.to_string()))?;
        self.writer
            .flush()
            .map_err(|e| GaipError::Json(e.to_string()))?;
        Ok(payload.len())
    }

    /// Block for the next message (subject, wire message).
    pub fn next(&mut self) -> std::io::Result<Option<(String, WireMessage)>> {
        loop {
            let mut line = String::new();
            if self.reader.read_line(&mut line)? == 0 {
                return Ok(None);
            }
            let line = line.trim();
            if line == "PING" {
                self.writer.write_all(b"PONG\r\n")?;
                self.writer.flush()?;
                continue;
            }
            if line.starts_with("-ERR") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("bus error: {line}"),
                ));
            }
            if !line.starts_with("MSG ") {
                continue; // +OK / INFO / PONG
            }
            // MSG <subject> <sid> [reply-to] <#bytes>
            let parts: Vec<&str> = line.split_whitespace().collect();
            let subject = parts.get(1).unwrap_or(&"").to_string();
            let len: usize = parts
                .last()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad MSG"))?;
            // read exactly len bytes of payload, then the CRLF that frames it
            let mut payload = vec![0u8; len];
            self.reader.read_exact(&mut payload)?;
            let mut crlf = [0u8; 2];
            let _ = self.reader.read_exact(&mut crlf);
            let msg = decode_wire(&payload).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("GAIP frame: {e}"))
            })?;
            match msg {
                Some((m, _used)) => return Ok(Some((subject, m))),
                None => continue, // a truncated frame: ask for the next one
            }
        }
    }

    /// Publish on `subject` and wait for the first message on `reply`.
    pub fn request(
        &mut self,
        subject: &str,
        reply: &str,
        msg: &WireMessage,
    ) -> Result<Option<(String, WireMessage)>, GaipError> {
        self.subscribe(reply)
            .map_err(|e| GaipError::Json(e.to_string()))?;
        self.publish(subject, msg)?;
        self.next().map_err(|e| GaipError::Json(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaip_core::Envelope;
    use serde_json::json;
    use std::net::TcpListener;

    /// A protocol-accurate NATS stand-in: enough of the wire to prove the
    /// client's handshake, SUB/PUB framing and MSG parsing.
    fn fake_bus() -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut log = Vec::new();
            s.write_all(b"INFO {\"server_id\":\"fake\",\"max_payload\":1048576}\r\n")
                .unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut line = String::new();
            // CONNECT
            r.read_line(&mut line).unwrap();
            log.push(line.trim().to_string());
            line.clear();
            // PING
            r.read_line(&mut line).unwrap();
            log.push(line.trim().to_string());
            s.write_all(b"PONG\r\n").unwrap();
            // SUB
            line.clear();
            r.read_line(&mut line).unwrap();
            log.push(line.trim().to_string());
            // (client sent PING after SUB)
            line.clear();
            r.read_line(&mut line).unwrap();
            log.push(line.trim().to_string());
            s.write_all(b"PONG\r\n").unwrap();
            // PUB <subject> <len> then payload
            line.clear();
            r.read_line(&mut line).unwrap();
            log.push(line.trim().to_string());
            let len: usize = line.split_whitespace().last().unwrap().parse().unwrap();
            let mut payload = vec![0u8; len + 2];
            r.read_exact(&mut payload).unwrap();
            // echo it back as a MSG on the same subject
            s.write_all(format!("MSG test.subject 1 {len}\r\n").as_bytes())
                .unwrap();
            s.write_all(&payload[..len]).unwrap();
            s.write_all(b"\r\n").unwrap();
            log
        });
        (addr, handle)
    }

    fn sample() -> WireMessage {
        let env = Envelope::task_request(
            "msg_1",
            "task_1",
            "urn:agent:python:langgraph:planner",
            "urn:agent:rust:tokio:engine",
            json!({"intent": "X"}),
        );
        WireMessage::request(3, env)
    }

    #[test]
    fn handshake_subscribe_publish_and_receive() {
        let (addr, handle) = fake_bus();
        let mut b = NatsBinding::connect(&addr).expect("handshake");
        assert!(b.server_info().contains("max_payload"));
        b.subscribe("test.subject").expect("subscribe");
        let sent = b.publish("test.subject", &sample()).expect("publish");
        assert!(sent > 10);
        let (subject, msg) = b.next().expect("next").expect("a message");
        assert_eq!(subject, "test.subject");
        assert_eq!(msg.request_id(), Some(3));
        let log = handle.join().unwrap();
        assert!(log.iter().any(|l| l.starts_with("CONNECT {")), "{log:?}");
        assert!(log.iter().any(|l| l == "SUB test.subject 1"), "{log:?}");
        assert!(
            log.iter().any(|l| l.starts_with("PUB test.subject ")),
            "{log:?}"
        );
    }

    #[test]
    fn a_broken_server_is_reported_not_guessed() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let _ = s.write_all(b"HTTP/1.1 400 Bad Request\r\n");
        });
        let err = match NatsBinding::connect(&addr) {
            Ok(_) => panic!("a non-NATS server must not be accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("INFO"), "{err}");
    }
}
