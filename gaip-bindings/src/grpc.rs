//! gRPC binding: GAIP over HTTP/2.
//!
//! The transport mapping, stated once:
//!
//! | gRPC | GAIP |
//! |---|---|
//! | unary `Send` | one `Request` in, one `Reply` out |
//! | bidi `Stream` | `TASK_STREAM` events in both directions |
//!
//! A `WireFrame.gaip` field carries the **same bytes** the framed TCP
//! binding carries (`encode_wire`), so switching bindings never touches
//! the envelope handling: gRPC is a pipe, not a second protocol.
//!
//! What this binding adds over TCP is what HTTP/2 adds: multiplexed
//! streams on one connection, header compression, and proxy friendliness.

use gaip_core::codec::{decode_wire, encode_wire};
use gaip_core::{GaipError, WireMessage};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod pb {
    tonic::include_proto!("gaip.v1");
}

use pb::gaip_transport_server::{GaipTransport, GaipTransportServer};
use pb::WireFrame;

/// What an application does with an inbound Request: produce a Reply.
pub type Handler = Arc<dyn Fn(WireMessage) -> Option<WireMessage> + Send + Sync>;

#[derive(Clone)]
pub struct GaipService {
    handler: Handler,
}

impl GaipService {
    pub fn new(handler: Handler) -> Self {
        Self { handler }
    }
}

fn to_frame(msg: &WireMessage, subject: &str) -> Result<WireFrame, Status> {
    let gaip = encode_wire(msg).map_err(|e| Status::internal(e.to_string()))?;
    Ok(WireFrame {
        gaip,
        subject: subject.to_string(),
    })
}

fn from_frame(frame: &WireFrame) -> Result<WireMessage, Status> {
    match decode_wire(&frame.gaip) {
        Ok(Some((m, _))) => Ok(m),
        Ok(None) => Err(Status::invalid_argument("truncated GAIP frame")),
        Err(e) => Err(Status::invalid_argument(format!("GAIP frame: {e}"))),
    }
}

#[tonic::async_trait]
impl GaipTransport for GaipService {
    async fn send(&self, request: Request<WireFrame>) -> Result<Response<WireFrame>, Status> {
        let frame = request.into_inner();
        let msg = from_frame(&frame)?;
        match (self.handler)(msg) {
            Some(reply) => Ok(Response::new(to_frame(&reply, &frame.subject)?)),
            None => Err(Status::failed_precondition(
                "the participant produced no reply (oneway request)",
            )),
        }
    }

    type StreamStream = ReceiverStream<Result<WireFrame, Status>>;

    /// Bidirectional stream: every inbound frame is answered on the same
    /// stream, which is how `TASK_STREAM` progress travels without
    /// another connection.
    async fn stream(
        &self,
        request: Request<tonic::Streaming<WireFrame>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let handler = self.handler.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            while let Ok(Some(frame)) = inbound.message().await {
                let Ok(msg) = from_frame(&frame) else { break };
                if let Some(reply) = handler(msg) {
                    match to_frame(&reply, &frame.subject) {
                        Ok(f) => {
                            if tx.send(Ok(f)).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Serve the binding until the returned future is dropped.
pub async fn serve(
    addr: std::net::SocketAddr,
    handler: Handler,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Server::builder()
        .add_service(GaipTransportServer::new(GaipService::new(handler)))
        .serve(addr)
        .await?;
    Ok(())
}

/// Client side of the binding.
pub struct GrpcBinding {
    inner: pb::gaip_transport_client::GaipTransportClient<tonic::transport::Channel>,
}

impl GrpcBinding {
    pub async fn connect(
        uri: impl Into<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let channel = tonic::transport::Endpoint::from_shared(uri.into())?
            .connect()
            .await?;
        Ok(Self {
            inner: pb::gaip_transport_client::GaipTransportClient::new(channel),
        })
    }

    /// Unary request/reply: the gRPC shape of the framed `Request`.
    pub async fn send(&mut self, msg: &WireMessage) -> Result<WireMessage, GaipError> {
        let frame = to_frame(msg, "gaip").map_err(|e| GaipError::Json(e.to_string()))?;
        let reply = self
            .inner
            .send(frame)
            .await
            .map_err(|e| GaipError::Json(e.to_string()))?
            .into_inner();
        from_frame(&reply).map_err(|e| GaipError::Json(e.to_string()))
    }

    /// Open the bidi stream and return its halves.
    pub async fn open_stream(
        &mut self,
    ) -> Result<
        (
            tokio::sync::mpsc::Sender<WireFrame>,
            tonic::Streaming<WireFrame>,
        ),
        GaipError,
    > {
        let (tx, rx) = tokio::sync::mpsc::channel::<WireFrame>(32);
        let stream = self
            .inner
            .stream(ReceiverStream::new(rx))
            .await
            .map_err(|e| GaipError::Json(e.to_string()))?
            .into_inner();
        Ok((tx, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaip_core::{Envelope, MessageType, ReplyStatus};
    use std::time::Duration;

    fn reply_for(msg: WireMessage) -> Option<WireMessage> {
        match msg {
            WireMessage::Request {
                request_id, body, ..
            } => {
                let reply_env = body.reply_to("msg_reply", serde_json::json!({"status": "OK"}));
                let _ = MessageType::TaskReply;
                Some(WireMessage::Reply {
                    request_id,
                    status: ReplyStatus::NoException,
                    body: Some(reply_env),
                })
            }
            _ => None,
        }
    }

    /// Real HTTP/2 over a real socket: this is the binding test.
    #[tokio::test]
    async fn unary_send_carries_a_gaip_request_and_returns_a_reply() {
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let local = listener.local_addr().unwrap();
        drop(listener); // pick a free port, then let tonic bind it

        let handler: Handler = Arc::new(reply_for);
        tokio::spawn(async move {
            let _ = serve(local, handler).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut client = GrpcBinding::connect(format!("http://{local}"))
            .await
            .expect("connect over HTTP/2");
        let req = Envelope::task_request(
            "msg_1",
            "task_1",
            "urn:agent:python:langgraph:planner-v2",
            "urn:agent:rust:tokio:engine",
            serde_json::json!({"intent": "INVENTORY_OPTIMIZATION"}),
        );
        let reply = client
            .send(&WireMessage::request(9, req.clone()))
            .await
            .expect("unary send");
        match reply {
            WireMessage::Reply {
                request_id,
                status,
                body,
            } => {
                assert_eq!(request_id, 9);
                assert!(status.is_ok());
                let env = body.expect("a reply envelope");
                assert_eq!(env.correlation_id, req.correlation_id);
                assert_eq!(env.message_type, MessageType::TaskReply);
            }
            other => panic!("expected a Reply, got {other:?}"),
        }
    }

    /// The bidi stream is what carries TASK_STREAM progress.
    #[tokio::test]
    async fn bidi_stream_answers_every_frame_on_the_same_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        drop(listener);

        let handler: Handler = Arc::new(reply_for);
        tokio::spawn(async move {
            let _ = serve(local, handler).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut client = GrpcBinding::connect(format!("http://{local}"))
            .await
            .expect("connect");
        let (tx, mut rx) = client.open_stream().await.expect("stream");

        for i in 0..3u32 {
            let env = Envelope::new(
                format!("msg_{i}"),
                "task_stream",
                "urn:agent:rust:tokio:engine",
                "urn:agent:python:langgraph:planner",
                MessageType::TaskStream,
                serde_json::json!({"progress": (i + 1) * 33}),
            );
            let frame = to_frame(&WireMessage::request(100 + i, env), "gaip.task").unwrap();
            tx.send(frame).await.expect("send frame");
        }
        drop(tx);

        let mut answered = 0;
        while let Ok(Some(frame)) = rx.message().await {
            let msg = from_frame(&frame).expect("a GAIP frame");
            if let WireMessage::Reply { request_id, .. } = msg {
                assert!((100..103).contains(&request_id));
                answered += 1;
            }
        }
        assert_eq!(answered, 3, "every streamed frame must be answered");
    }

    #[tokio::test]
    async fn a_frame_that_is_not_a_gaip_message_is_refused_with_invalid_argument() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        drop(listener);
        let handler: Handler = Arc::new(reply_for);
        tokio::spawn(async move {
            let _ = serve(local, handler).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{local}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut raw = pb::gaip_transport_client::GaipTransportClient::new(channel);
        let err = raw
            .send(WireFrame {
                gaip: b"\x00\x00\x00\x05junk!".to_vec(),
                subject: "gaip".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "{}",
            err.message()
        );
    }
}
