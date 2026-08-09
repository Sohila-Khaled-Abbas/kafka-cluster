# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please email: **[sohila.k.data@gmail.com]**

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

| Action | Timeframe |
| -------- | ----------- |
| Acknowledgment | Within 48 hours |
| Initial assessment | Within 1 week |
| Patch release | Within 2 weeks |

---

## Security Best Practices for This Cluster

> ⚠️ This cluster is configured for **local development only**.

### Current Security Posture

| Setting | Value | Risk Level |
| --------- | ------- | ------------ |
| `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND` | `true` | 🔴 High — No authorization |
| Listener protocol | `PLAINTEXT` | 🟡 Medium — No encryption |
| ZooKeeper authentication | None | 🟡 Medium — Open access |
| Network | External Docker network | 🟢 Low — Container isolated |

### Before Going to Production

- [ ] Enable SASL/SCRAM or mTLS authentication
- [ ] Configure SSL/TLS for all listeners
- [ ] Set `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: false`
- [ ] Define explicit ACLs for producers and consumers
- [ ] Enable ZooKeeper authentication (SASL)
- [ ] Restrict the Docker network
- [ ] Set up monitoring and alerting
- [ ] Enable audit logging

See [docs/SECURITY.md](docs/SECURITY.md) for detailed hardening instructions.
