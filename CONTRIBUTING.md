# Contributing to async-agent-dispatcher

We welcome community contributions, architectural improvements, and bug fixes. To ensure stability, concurrency safety, and zero data loss under production load, all contributions must adhere to our SRE standards.

---

## 🛠️ Development Workflow

No host Python or Redis setup is required. All test execution and verification run through Docker:

```bash
# Display all available targets
make help

# Build local container image
make build

# Run complete test suite in isolated Redis container
make test

# Run static clean-room leak scan
make check-blacklist

# Run container security & vulnerability scan
make audit

# Clean up dangling containers and volumes
make clean
```

---

## 🛡️ SRE Architecture Rules

1. **Deterministic Idempotency:** Every dispatch operation must support explicit, deterministic job IDs. Duplicate submissions within the active execution window must be safely deduplicated.
2. **Memory Safety & Process Isolation:** Never execute unbounded worker tasks directly in thread-shared memory. Always wrap subprocess streams in `BoundedStringIO` with strict memory ceilings (default: 5MB).
3. **Fork Safety:** Always clear cached Redis connection pools across process boundaries via `os.register_at_fork`.
4. **Zero Lost Tasks:** State transitions in Dead Letter Queues must be atomic. Never use `os.replace` on open queue files—always truncate and write in-place under POSIX `LOCK_EX` to preserve the file inode for concurrent writers.
5. **Clean-Room Verification:** All code must pass `./scripts/check_blacklist.sh` prior to commit. Never include proprietary tenant IDs, internal endpoints, or hardcoded secrets.

---

## 👥 Contributors

- **Emmanuel Ortiz** ([@emmanuelol](https://github.com/emmanuelol)) - Core Architecture & SRE
- **Carlos Ortiz** ([@carlosaol](https://github.com/carlosaol)) - Systems Engineering & Concurrency Design

---

## 📄 Pull Request Checklist

- [ ] All 22+ tests pass cleanly via `make test`.
- [ ] No blacklisted tokens detected via `make check-blacklist`.
- [ ] Container image builds and audits clean via `make audit`.
- [ ] New public APIs include type hints and docstrings.
