# Reset dagens Repository and Push Migrated Code

⚠️ **WARNING: This will PERMANENTLY DELETE all content and history in github.com/seyi/dagens**

This guide will help you:
1. Completely wipe the dagens repository (remove all commits and content)
2. Push the migrated spark-ai-agents code as a fresh start

## Prerequisites

- GitHub authentication configured (SSH or HTTPS token)
- Git installed locally
- Access to github.com/seyi/dagens

## Option 1: Automated Script (Recommended)

Save and run the `reset_dagens.sh` script:

```bash
cd /path/to/spark
chmod +x reset_dagens.sh
./reset_dagens.sh
```

The script will:
1. Create a clean copy of spark-ai-agents
2. Initialize fresh git repository
3. Create initial commit
4. Force push to dagens (wiping existing content)

## Option 2: Manual Steps

### Step 1: Wipe dagens Repository

```bash
# Clone dagens (or use existing clone)
cd /tmp
git clone https://github.com/seyi/dagens.git dagens-reset
cd dagens-reset

# Remove all git history
rm -rf .git

# Initialize fresh repository
git init
git branch -M main

# Create empty commit to establish history
git commit --allow-empty -m "Initial commit - preparing for migration"

# Add remote and force push (DESTRUCTIVE!)
git remote add origin https://github.com/seyi/dagens.git
git push -u origin main --force
```

### Step 2: Push Migrated Content

```bash
# Go to your spark repository
cd /path/to/spark/spark-ai-agents

# Copy all migrated content to dagens
cd /tmp/dagens-reset
cp -r /path/to/spark/spark-ai-agents/* .
cp /path/to/spark/spark-ai-agents/.gitignore . 2>/dev/null || true
cp -r /path/to/spark/spark-ai-agents/.github . 2>/dev/null || true

# Stage all files
git add .

# Create initial commit with full project
git commit -m "Initial commit: Dagens AI Agents framework

Complete production-ready AI agent framework featuring:

Architecture:
- Distributed agent execution with task delegation
- DAG-based workflow orchestration
- Support for LLM, human-in-the-loop, and workflow agents

Production Hardening:
- Resilience patterns: circuit breakers, retry logic, rate limiting
- State management: multiple backends (memory, file, S3, Redis)
- Observability: structured logging, Prometheus metrics, distributed tracing

Workflow Agents (ADK-Compatible):
- Sequential agents for linear workflows
- Parallel agents for concurrent execution
- Loop agents for iterative processing
- MapReduce, Router, and Conditional agents

Features:
- MCP (Model Context Protocol) tool integration
- Python bindings for cross-language support
- Comprehensive examples and documentation
- Kubernetes deployment ready

Migrated from apache/spark spark-ai-agents project.

Module: github.com/seyi/dagens
Python Package: dagens
License: Apache 2.0"

# Force push to replace everything
git push origin main --force
```

## Option 3: GitHub Web Interface + Local Push

1. **Via GitHub Web**:
   - Go to https://github.com/seyi/dagens/settings
   - Scroll to "Danger Zone"
   - Click "Delete this repository"
   - Type "seyi/dagens" to confirm
   - Create a new empty repository named "dagens"

2. **Push Local Code**:
   ```bash
   cd /tmp
   mkdir dagens-new
   cd dagens-new

   # Copy migrated content
   cp -r /path/to/spark/spark-ai-agents/* .
   cp /path/to/spark/spark-ai-agents/.gitignore .

   # Initialize and push
   git init
   git branch -M main
   git add .
   git commit -m "Initial commit: Dagens AI Agents framework"
   git remote add origin https://github.com/seyi/dagens.git
   git push -u origin main
   ```

## Verification

After pushing, verify at https://github.com/seyi/dagens:

1. ✅ Repository contains migrated code
2. ✅ `go.mod` shows `module github.com/seyi/dagens`
3. ✅ README displays "Dagens AI Agents"
4. ✅ All import paths reference `github.com/seyi/dagens`
5. ✅ Examples directory has production hardening examples
6. ✅ Only one commit in history (fresh start)

## What Gets Pushed

The complete Dagens AI Agents framework:

```
dagens/
├── cmd/                    # Command-line tools
├── docs/                   # Documentation
├── examples/              # Production examples
├── pkg/
│   ├── agent/            # Core agent framework
│   ├── agents/           # Workflow agents (Sequential, Parallel, Loop)
│   ├── coordinator/      # Distributed coordination
│   ├── executor/         # Execution engine
│   ├── observability/    # Logging, metrics, tracing
│   ├── resilience/       # Circuit breakers, retries
│   ├── runtime/          # Event runtime
│   ├── scheduler/        # DAG scheduler
│   ├── state/            # State management
│   └── tools/            # MCP tools integration
├── python/               # Python bindings
├── tests/                # Integration tests
├── go.mod               # Module: github.com/seyi/dagens
├── go.sum
└── README.md            # Project documentation
```

## Rollback (Emergency Only)

If you need to rollback:

```bash
# Clone the current dagens before wiping (backup)
git clone https://github.com/seyi/dagens.git dagens-backup
cd dagens-backup
git bundle create ../dagens-backup.bundle --all

# If you need to restore
cd dagens-repo
git fetch ../dagens-backup.bundle
git reset --hard FETCH_HEAD
git push origin main --force
```

## Security Notes

- This uses `--force` push which is DESTRUCTIVE
- All existing issues, PRs, and commits will be preserved (GitHub keeps them for 90 days)
- Repository settings (collaborators, webhooks, etc.) are preserved
- Only the git history and files are replaced

## Support

If you encounter authentication issues:
- HTTPS: Use a Personal Access Token (PAT) with `repo` scope
- SSH: Ensure your SSH key is added to GitHub
- Check: `git config --global user.name` and `user.email` are set

---

**Ready to proceed?** Run `./reset_dagens.sh` or follow the manual steps above.
