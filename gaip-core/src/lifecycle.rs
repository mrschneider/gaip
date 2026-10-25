//! The task lifecycle: which primitive may follow which.
//!
//! A closed vocabulary is not enough. Six verbs can still be spoken in an
//! order that loses work: a `TASK_REPLY` after a `CANCEL_REQUEST`, a
//! second `TASK_REQUEST` reusing a live `correlation_id`, a `TASK_STREAM`
//! for a task nobody opened. GIOP got this right by making the object
//! reference stateful; here the state lives in a registry keyed by
//! `correlation_id`.
//!
//! ```text
//!            TASK_REQUEST
//!                 |
//!                 v
//!   +-----------> Requested --TASK_STREAM--> Streaming --TASK_REPLY--> Replied
//!   |                 |                        |
//!   |                 +----HUMAN_INTERVENT-----+--> AwaitingHuman
//!   |                 |                        |         |
//!   +--(any pre-terminal state)--CANCEL_REQUEST-------->  Cancelled
//! ```
//!
//! Terminal states are terminal: after `Replied` or `Cancelled`, further
//! traffic for that correlation is refused with the state named.

use crate::envelope::{Envelope, MessageType};
use crate::error::GaipError;
use crate::urn::AgentUrn;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskState {
    /// Accepted, nothing streamed yet.
    Requested,
    /// At least one TASK_STREAM has been sent.
    Streaming,
    /// Paused for a human decision.
    AwaitingHuman,
    /// Terminal: a TASK_REPLY landed.
    Replied,
    /// Terminal: a CANCEL_REQUEST landed.
    Cancelled,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Replied | Self::Cancelled)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Requested => "Requested",
            Self::Streaming => "Streaming",
            Self::AwaitingHuman => "AwaitingHuman",
            Self::Replied => "Replied",
            Self::Cancelled => "Cancelled",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub requester: AgentUrn,
    pub responder: AgentUrn,
    pub state: TaskState,
    pub streams: u32,
    pub notes: Vec<String>,
}

/// Per-participant view of the tasks it is involved in.
#[derive(Debug, Default)]
pub struct TaskRegistry {
    tasks: BTreeMap<String, Task>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id)
    }

    pub fn open_tasks(&self) -> impl Iterator<Item = (&String, &Task)> {
        self.tasks.iter().filter(|(_, t)| !t.state.is_terminal())
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Apply one envelope to the registry. This is the single gate: a
    /// participant that calls it cannot act on an out-of-order message.
    pub fn accept(&mut self, env: &Envelope) -> Result<&Task, GaipError> {
        env.validate()?;
        let sender: AgentUrn = env.sender.parse()?;
        let recipient: AgentUrn = env.recipient.parse()?;
        let id = env.correlation_id.clone();

        match env.message_type {
            MessageType::TaskRequest => {
                if self.tasks.contains_key(&id) {
                    return Err(GaipError::TaskExists(id));
                }
                self.tasks.insert(
                    id.clone(),
                    Task {
                        id: id.clone(),
                        requester: sender,
                        responder: recipient,
                        state: TaskState::Requested,
                        streams: 0,
                        notes: Vec::new(),
                    },
                );
                Ok(self.tasks.get(&id).expect("just inserted"))
            }
            // A capability query is stateless introspection: it may or may
            // not correlate to a task, and it never changes one.
            MessageType::CapabilityQuery => {
                if let Some(task) = self.tasks.get_mut(&id) {
                    task.notes.push("capability_query".to_string());
                }
                Ok(self.tasks.entry(id.clone()).or_insert_with(|| Task {
                    id: id.clone(),
                    requester: sender,
                    responder: recipient,
                    state: TaskState::Requested,
                    streams: 0,
                    notes: vec!["capability_query (stateless)".to_string()],
                }))
            }
            MessageType::TaskStream => {
                let task = self.task_mut(&id)?;
                if task.state.is_terminal() {
                    return Err(GaipError::IllegalTransition {
                        task: id,
                        from: task.state,
                        primitive: env.message_type,
                    });
                }
                if task.state == TaskState::AwaitingHuman {
                    return Err(GaipError::IllegalTransition {
                        task: id,
                        from: task.state,
                        primitive: env.message_type,
                    });
                }
                task.state = TaskState::Streaming;
                task.streams += 1;
                Ok(self.tasks.get(&id).expect("present"))
            }
            MessageType::TaskReply => {
                let task = self.task_mut(&id)?;
                // the CANCELLED acknowledgement is the ONE thing a
                // terminal-from-cancel task still accepts
                if task.state == TaskState::Cancelled {
                    let ack = env
                        .payload
                        .get("status")
                        .and_then(|s| s.as_str())
                        .map(|s| s.eq_ignore_ascii_case("CANCELLED"))
                        .unwrap_or(false);
                    if !ack {
                        return Err(GaipError::IllegalTransition {
                            task: id,
                            from: task.state,
                            primitive: env.message_type,
                        });
                    }
                    task.state = TaskState::Replied;
                    return Ok(self.tasks.get(&id).expect("present"));
                }
                if task.state.is_terminal() {
                    return Err(GaipError::IllegalTransition {
                        task: id,
                        from: task.state,
                        primitive: env.message_type,
                    });
                }
                match task.state {
                    TaskState::Cancelled => unreachable!("handled above"),
                    // A paused task is RESOLVED, not completed: the human
                    // decision carries `decision`, and the work resumes.
                    TaskState::AwaitingHuman if env.payload.get("decision").is_some() => {
                        task.state = TaskState::Streaming;
                        task.notes.push("human_decision".to_string());
                        return Ok(self.tasks.get(&id).expect("present"));
                    }
                    _ => {}
                }
                task.state = TaskState::Replied;
                Ok(self.tasks.get(&id).expect("present"))
            }
            MessageType::CancelRequest => {
                let task = self.task_mut(&id)?;
                if task.state.is_terminal() {
                    return Err(GaipError::IllegalTransition {
                        task: id,
                        from: task.state,
                        primitive: env.message_type,
                    });
                }
                task.state = TaskState::Cancelled;
                Ok(self.tasks.get(&id).expect("present"))
            }
            MessageType::HumanIntervent => {
                let task = self.task_mut(&id)?;
                if task.state.is_terminal() {
                    return Err(GaipError::IllegalTransition {
                        task: id,
                        from: task.state,
                        primitive: env.message_type,
                    });
                }
                task.state = TaskState::AwaitingHuman;
                task.notes.push("human_intervent".to_string());
                Ok(self.tasks.get(&id).expect("present"))
            }
        }
    }

    fn task_mut(&mut self, id: &str) -> Result<&mut Task, GaipError> {
        self.tasks
            .get_mut(id)
            .ok_or_else(|| GaipError::NoSuchTask(id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const REQ: &str = "urn:agent:python:langgraph:planner-v2";
    const ENG: &str = "urn:agent:rust:tokio:analytics-engine-v1";

    fn req(id: &str) -> Envelope {
        Envelope::task_request("msg_1", id, REQ, ENG, json!({"intent": "X"}))
    }

    fn from(env: &Envelope, mt: MessageType, msg: &str) -> Envelope {
        Envelope::new(
            msg,
            env.correlation_id.clone(),
            env.recipient.clone(),
            env.sender.clone(),
            mt,
            json!({"ok": true}),
        )
    }

    #[test]
    fn the_happy_path_request_stream_stream_reply() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        assert_eq!(r.get("task_1").unwrap().state, TaskState::Requested);

        let s1 = from(&request, MessageType::TaskStream, "m2");
        assert_eq!(r.accept(&s1).unwrap().state, TaskState::Streaming);
        let s2 = from(&request, MessageType::TaskStream, "m3");
        assert_eq!(r.accept(&s2).unwrap().streams, 2);

        let reply = from(&request, MessageType::TaskReply, "m4");
        assert_eq!(r.accept(&reply).unwrap().state, TaskState::Replied);
        assert_eq!(
            r.open_tasks().count(),
            0,
            "terminal tasks are not open work"
        );
    }

    #[test]
    fn a_reused_correlation_id_is_refused_while_open() {
        let mut r = TaskRegistry::new();
        r.accept(&req("task_1")).unwrap();
        let err = r.accept(&req("task_1")).unwrap_err();
        assert!(matches!(err, GaipError::TaskExists(_)), "{err}");
    }

    #[test]
    fn cancel_then_reply_is_an_illegal_transition() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        r.accept(&from(&request, MessageType::CancelRequest, "m2"))
            .unwrap();
        let err = r
            .accept(&from(&request, MessageType::TaskReply, "m3"))
            .unwrap_err();
        match err {
            GaipError::IllegalTransition { from, .. } => {
                assert_eq!(from, TaskState::Cancelled)
            }
            other => panic!("expected IllegalTransition, got {other:?}"),
        }
    }

    #[test]
    fn cancel_after_reply_is_refused_too() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        r.accept(&from(&request, MessageType::TaskReply, "m2"))
            .unwrap();
        let err = r
            .accept(&from(&request, MessageType::CancelRequest, "m3"))
            .unwrap_err();
        assert!(err.to_string().contains("Replied"), "{err}");
    }

    #[test]
    fn stream_without_a_request_is_refused() {
        let mut r = TaskRegistry::new();
        let ghost = Envelope::new(
            "m1",
            "task_ghost",
            ENG,
            REQ,
            MessageType::TaskStream,
            json!({}),
        );
        let err = r.accept(&ghost).unwrap_err();
        assert!(matches!(err, GaipError::NoSuchTask(_)), "{err}");
    }

    #[test]
    fn human_intervent_pauses_and_a_reply_resumes() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        r.accept(&from(&request, MessageType::TaskStream, "m2"))
            .unwrap();
        let pause = from(&request, MessageType::HumanIntervent, "m3");
        assert_eq!(r.accept(&pause).unwrap().state, TaskState::AwaitingHuman);

        // a stream while paused is refused: the task is not running
        let err = r
            .accept(&from(&request, MessageType::TaskStream, "m4"))
            .unwrap_err();
        assert!(matches!(err, GaipError::IllegalTransition { .. }), "{err}");

        // the human decision comes back as a TASK_REPLY
        let resume = from(&request, MessageType::TaskReply, "m5");
        assert_eq!(r.accept(&resume).unwrap().state, TaskState::Replied);
    }

    #[test]
    fn cancel_is_acknowledged_by_exactly_one_status_cancelled_reply() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        r.accept(&from(&request, MessageType::CancelRequest, "m2"))
            .unwrap();
        // a normal reply after a cancel is refused
        assert!(r
            .accept(&from(&request, MessageType::TaskReply, "m3"))
            .is_err());
        // the ack is accepted once...
        let mut ack = from(&request, MessageType::TaskReply, "m4");
        ack.payload = json!({"status": "CANCELLED"});
        assert_eq!(r.accept(&ack).unwrap().state, TaskState::Replied);
        // ...and only once
        assert!(r.accept(&ack).is_err(), "terminal states stay terminal");
    }

    #[test]
    fn a_human_decision_resumes_instead_of_completing() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        r.accept(&from(&request, MessageType::HumanIntervent, "m2"))
            .unwrap();
        let mut decision = from(&request, MessageType::TaskReply, "m3");
        decision.payload = json!({"decision": "APPROVE"});
        assert_eq!(
            r.accept(&decision).unwrap().state,
            TaskState::Streaming,
            "a decision resumes the work, it does not finish it"
        );
        assert!(r
            .get("task_1")
            .unwrap()
            .notes
            .contains(&"human_decision".to_string()));
        // the work then finishes normally
        assert_eq!(
            r.accept(&from(&request, MessageType::TaskReply, "m4"))
                .unwrap()
                .state,
            TaskState::Replied
        );
    }

    #[test]
    fn capability_query_is_stateless_and_never_mutates_a_task() {
        let mut r = TaskRegistry::new();
        let request = req("task_1");
        r.accept(&request).unwrap();
        let query = Envelope::new(
            "m2",
            "task_1",
            REQ,
            ENG,
            MessageType::CapabilityQuery,
            json!({}),
        );
        r.accept(&query).unwrap();
        assert_eq!(
            r.get("task_1").unwrap().state,
            TaskState::Requested,
            "introspection must not move the lifecycle"
        );
        assert_eq!(
            r.get("task_1").unwrap().notes,
            vec!["capability_query".to_string()]
        );
    }

    #[test]
    fn accept_validates_the_envelope_too() {
        let mut r = TaskRegistry::new();
        let mut bad = req("task_1");
        bad.protocol_version = "9.9".into();
        assert!(r.accept(&bad).is_err());
        assert!(
            r.get("task_1").is_none(),
            "a refused message must not create state"
        );
    }
}
