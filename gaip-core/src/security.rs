//! CSIv2, reduced to what a library can honestly provide: a signed
//! **identity assertion** carried in a service context.
//!
//! CSIv2 (Common Secure Interoperability v2) established a security
//! context at the transport layer (TLS/GSSAPI) and then let the client
//! *assert* an identity for the request — including on behalf of another
//! principal (delegation). The transport half belongs to mTLS; the
//! assertion half is a token, and a token is code.
//!
//! What this module implements:
//! * [`IdentityAssertion`] — subject, roles, optional delegation
//!   (`on_behalf_of`), absolute expiry.
//! * [`Credentials`] — HMAC-SHA256 signing/verification with a shared
//!   key, plus expiry and role checks.
//!
//! What it deliberately does NOT do: TLS. Deploy it *inside*mTLS (or a
//! private network) exactly as CSIv2 assumed a secure transport. An
//! assertion proves "this peer was issued this identity"; it cannot prove
//! who is on the wire without the transport doing its half.

use crate::context::now_ms;
use crate::context::{id, ServiceContext};
use crate::error::GaipError;
use crate::message::ReplyStatus;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// The identity a peer asserts for a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityAssertion {
    /// The authenticated principal (`agent:planner-v2`, `svc:orchestrator`).
    pub subject: String,
    pub roles: Vec<String>,
    /// Set when this principal acts for another (CSIv2 delegation).
    pub on_behalf_of: Option<String>,
    /// Absolute expiry, unix ms. A token without an expiry is a password.
    pub expires_at_ms: u128,
}

impl IdentityAssertion {
    pub fn new(subject: impl Into<String>, roles: &[&str], ttl_ms: u64) -> Self {
        Self {
            subject: subject.into(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            on_behalf_of: None,
            expires_at_ms: now_ms() + ttl_ms as u128,
        }
    }

    pub fn delegating(mut self, principal: impl Into<String>) -> Self {
        self.on_behalf_of = Some(principal.into());
        self
    }

    /// The bytes the signature covers. Field order is part of the
    /// contract: changing it invalidates every issued token.
    fn canonical(&self) -> String {
        format!(
            "v1|{}|{}|{}|{}",
            self.subject,
            self.roles.join(","),
            self.on_behalf_of.clone().unwrap_or_default(),
            self.expires_at_ms
        )
    }

    pub fn expired(&self) -> bool {
        now_ms() >= self.expires_at_ms
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 0x0f) as usize] as char);
    }
    s
}

/// Signing and verification with a shared key.
#[derive(Clone)]
pub struct Credentials {
    key: Vec<u8>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never print the key, even in a panic message
        f.write_str("Credentials(<redacted>)")
    }
}

impl Credentials {
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self { key: key.into() }
    }

    fn mac(&self, canonical: &str) -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(&self.key).expect("hmac accepts any key length");
        m.update(canonical.as_bytes());
        m.finalize().into_bytes().to_vec()
    }

    /// Sign an assertion into a service context (CSIv2 identity token).
    pub fn assert(&self, id: &IdentityAssertion) -> ServiceContext {
        let sig = hex(&self.mac(&id.canonical()));
        let payload = format!(
            r#"{{"subject":"{}","roles":[{}],"on_behalf_of":{},"expires_at_ms":{},"sig":"{}"}}"#,
            id.subject,
            id.roles
                .iter()
                .map(|r| format!("\"{r}\""))
                .collect::<Vec<_>>()
                .join(","),
            match &id.on_behalf_of {
                Some(p) => format!("\"{p}\""),
                None => "null".to_string(),
            },
            id.expires_at_ms,
            sig
        );
        ServiceContext::new(id::IDENTITY, payload)
    }

    /// Verify an identity token: signature first, then expiry. A token
    /// that does not verify is refused, never "best-effort accepted".
    pub fn verify(&self, ctx: &ServiceContext) -> Result<IdentityAssertion, GaipError> {
        if ctx.id != id::IDENTITY {
            return Err(GaipError::BadField {
                field: "identity",
                why: format!("service context 0x{:04X} is not an identity token", ctx.id),
            });
        }
        let v: serde_json::Value =
            serde_json::from_str(&ctx.data).map_err(|e| GaipError::BadField {
                field: "identity",
                why: format!("token is not JSON: {e}"),
            })?;
        let get = |k: &str| -> Result<String, GaipError> {
            v.get(k)
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .ok_or_else(|| GaipError::BadField {
                    field: "identity",
                    why: format!("token has no '{k}'"),
                })
        };
        let assertion = IdentityAssertion {
            subject: get("subject")?,
            roles: v
                .get("roles")
                .and_then(|r| r.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            on_behalf_of: v
                .get("on_behalf_of")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            expires_at_ms: v
                .get("expires_at_ms")
                .and_then(|x| x.as_u64())
                .map(|x| x as u128)
                .ok_or_else(|| GaipError::BadField {
                    field: "identity",
                    why: "token has no expiry".to_string(),
                })?,
        };
        let claimed = get("sig")?;
        let expected = hex(&self.mac(&assertion.canonical()));
        // constant-time compare: a timing oracle on a token check is a
        // token forgery kit
        let ok = claimed.len() == expected.len()
            && claimed
                .bytes()
                .zip(expected.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        if !ok {
            return Err(GaipError::BadField {
                field: "identity",
                why: "signature does not verify".to_string(),
            });
        }
        if assertion.expired() {
            return Err(GaipError::BadField {
                field: "identity",
                why: "identity token expired".to_string(),
            });
        }
        Ok(assertion)
    }
}

/// Authorization: a verified identity must hold the required role.
/// Returns the reply to send when it does not.
pub fn authorize(id: &IdentityAssertion, required_role: &str) -> Result<(), ReplyStatus> {
    if id.has_role(required_role) {
        Ok(())
    } else {
        Err(ReplyStatus::user(
            "E_NO_PERMISSION",
            format!(
                "{} lacks role '{required_role}' (has: {})",
                id.subject,
                if id.roles.is_empty() {
                    "none".to_string()
                } else {
                    id.roles.join(",")
                }
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> Credentials {
        Credentials::new(b"shared-secret-key".to_vec())
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let c = creds();
        let id = IdentityAssertion::new("agent:planner-v2", &["planner", "reader"], 60_000);
        let ctx = c.assert(&id);
        assert_eq!(ctx.id, id::IDENTITY);
        let back = c.verify(&ctx).unwrap();
        assert_eq!(back.subject, "agent:planner-v2");
        assert_eq!(back.roles, vec!["planner", "reader"]);
    }

    #[test]
    fn a_tampered_subject_is_refused() {
        let c = creds();
        let ctx = c.assert(&IdentityAssertion::new("agent:low", &["reader"], 60_000));
        let forged =
            ServiceContext::new(id::IDENTITY, ctx.data.replace("agent:low", "agent:admin"));
        let err = c.verify(&forged).unwrap_err().to_string();
        assert!(err.contains("signature"), "{err}");
    }

    #[test]
    fn a_forged_role_is_refused() {
        let c = creds();
        let ctx = c.assert(&IdentityAssertion::new("agent:low", &["reader"], 60_000));
        let forged = ServiceContext::new(id::IDENTITY, ctx.data.replace("\"reader\"", "\"admin\""));
        assert!(c.verify(&forged).is_err());
    }

    #[test]
    fn another_key_cannot_verify() {
        let ctx = creds().assert(&IdentityAssertion::new("a", &["r"], 60_000));
        let other = Credentials::new(b"a-different-key".to_vec());
        assert!(
            other.verify(&ctx).is_err(),
            "a token is only valid under its issuer's key"
        );
    }

    #[test]
    fn an_expired_token_is_refused() {
        let c = creds();
        let mut id = IdentityAssertion::new("agent:slow", &["reader"], 1);
        id.expires_at_ms = now_ms().saturating_sub(1);
        let ctx = c.assert(&id);
        let err = c.verify(&ctx).unwrap_err().to_string();
        assert!(err.contains("expired"), "{err}");
    }

    #[test]
    fn delegation_is_carried_and_signed() {
        let c = creds();
        let id = IdentityAssertion::new("agent:worker", &["executor"], 60_000)
            .delegating("user:guilherme");
        let back = c.verify(&c.assert(&id)).unwrap();
        assert_eq!(back.on_behalf_of.as_deref(), Some("user:guilherme"));
        // tampering with the delegated principal breaks the signature
        let ctx = c.assert(&id);
        let forged =
            ServiceContext::new(id::IDENTITY, ctx.data.replace("user:guilherme", "user:ceo"));
        assert!(c.verify(&forged).is_err());
    }

    #[test]
    fn a_non_identity_context_is_not_a_token() {
        let c = creds();
        let ctx = ServiceContext::trace("trace-1");
        assert!(c.verify(&ctx).is_err());
    }

    #[test]
    fn role_authorization_returns_a_user_exception() {
        let id = IdentityAssertion::new("agent:reader", &["reader"], 60_000);
        assert!(authorize(&id, "reader").is_ok());
        let status = authorize(&id, "admin").unwrap_err();
        match status {
            ReplyStatus::UserException { code, message } => {
                assert_eq!(code, "E_NO_PERMISSION");
                assert!(message.contains("admin"), "{message}");
            }
            other => panic!("expected a user exception, got {other:?}"),
        }
    }
}
