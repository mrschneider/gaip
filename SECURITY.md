# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| latest release | yes |
| older releases | no |

## Reporting a vulnerability

Open a GitHub security advisory (Security → Report a vulnerability) or
email the maintainer. Do not open a public issue with exploit details.

Please include: the affected component (engine / broker / core /
bindings), a minimal reproduction (a script is ideal — the
`experiments/` harness is a good template), and the impact you see.

## Threat model notes

* Identity is a shared-secret HMAC assertion (CSIv2 token); transport is
  TLS 1.3. GSSAPI/Kerberos is not implemented.
* Brokers trust peer brokers by address — do not point `--peer` at an
  untrusted host.
* The dedupe cache (exactly-once effect) is bounded (4096 / 120 s);
  beyond the window a replay is a new message.
* Locks are per engine process with leases; a crashed holder cannot
  deadlock, but locks do not span brokers.
