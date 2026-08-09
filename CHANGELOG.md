# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.0] — 2026-08-09

### Added
- **Kafka UI** web dashboard on port `9021` (provectuslabs/kafka-ui)
- Architecture infographic and Mermaid diagrams (`docs/ARCHITECTURE.md`)
- Kafka UI usage guide (`docs/KAFKA-UI-GUIDE.md`)
- Spark Structured Streaming integration guide (`docs/SPARK-INTEGRATION.md`)
- Operations & troubleshooting guide (`docs/OPERATIONS.md`)
- Data pipeline patterns guide (`docs/DATA-PIPELINE-PATTERNS.md`)
- Security hardening guide (`docs/SECURITY.md`)
- Example producer, consumer, and Spark streaming scripts (`examples/`)
- Makefile for common operations
- `.editorconfig` for consistent formatting
- `SECURITY.md` vulnerability reporting policy

### Changed
- Updated README with Kafka UI section, docs index, and port reference

---

## [1.0.0] — 2026-08-09

### Added
- 3-broker Confluent Kafka cluster (`cp-kafka:7.3.2`)
- ZooKeeper with health checks and 4-letter word whitelist
- Multi-listener configuration (INTERNAL / EXTERNAL / DOCKER)
- Extended ZooKeeper session timeouts (60s) for stability under load
- Shared `itvdelabnw` Docker network for Spark integration
- Professional documentation (README, CONTRIBUTING, LICENSE)
- Helper scripts for startup (`scripts/start.sh`) and health monitoring (`scripts/health-check.sh`)
- `.gitignore` for Docker/Kafka projects
- `.env.example` environment template
