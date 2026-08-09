# Contributing to Kafka Cluster

Thank you for considering contributing! Here's how you can help.

---

## 🚀 Getting Started

1. **Fork** the repository
2. **Clone** your fork:
   ```bash
   git clone https://github.com/<your-username>/kafka-cluster.git
   cd kafka-cluster
   ```
3. **Create a branch** for your feature or fix:
   ```bash
   git checkout -b feature/my-awesome-feature
   ```

---

## 📋 Branch Naming Convention

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feature/<description>` | `feature/add-schema-registry` |
| Bug Fix | `fix/<description>` | `fix/zk-session-timeout` |
| Documentation | `docs/<description>` | `docs/update-readme` |
| Chore | `chore/<description>` | `chore/update-kafka-version` |

---

## 📝 Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]
```

**Examples:**
```
feat(docker): add Schema Registry service
fix(kafka): increase ZooKeeper session timeout to 60s
docs(readme): add Spark integration examples
chore(deps): bump Confluent Platform to 7.5.0
```

---

## 🔄 Pull Request Process

1. Ensure your changes work by running `docker compose up -d` and verifying all services start
2. Update `README.md` if your change affects usage or configuration
3. Submit a pull request with a clear description of the changes
4. Request review from a maintainer

---

## 🧪 Testing Checklist

Before submitting a PR, verify:

- [ ] `docker compose up -d` starts all services without errors
- [ ] `docker compose ps` shows all containers as `Up` and healthy
- [ ] ZooKeeper responds: `docker exec zoo1 bash -c "echo ruok | nc localhost 2181"`
- [ ] A test topic can be created and messages produced/consumed
- [ ] Spark can connect using the INTERNAL listener (if applicable)

---

## 📄 License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
