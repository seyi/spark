# Migration Guide: Moving to Independent Repository

## Overview

This guide documents how to migrate the spark-ai-agents project from `github.com/seyi/dagens` to an independent repository.

## Pre-Migration Assessment

### ✅ Independence Verification

**Code Dependencies:**
- ✅ Zero dependencies on Apache Spark core (Scala/Java)
- ✅ All imports are within `spark-ai-agents/` or external packages
- ✅ Self-contained Go module with own `go.mod`
- ✅ Independent test suite (152 Go files, all self-contained)

**External Dependencies:**
- Redis (optional, for production state backend)
- PostgreSQL (optional, for production state backend)
- Prometheus (for metrics)
- Standard Go/Python packages only

**Breaking Changes Required:**
1. Module import path: `github.com/seyi/dagens` → `github.com/seyi/spark-ai-agents`
2. Repository URL in documentation
3. Python package URL in `setup.py`

## Migration Script

Save this as `migrate.sh`:

```bash
#!/bin/bash
set -e

# Configuration
OLD_PATH="github.com/seyi/dagens"
seyi="${1:-your-org}"  # Pass new org as first argument
NEW_PATH="github.com/${seyi}/spark-ai-agents"

echo "=========================================="
echo "Spark AI Agents Migration Script"
echo "=========================================="
echo "Old path: ${OLD_PATH}"
echo "New path: ${NEW_PATH}"
echo ""

# Confirm
read -p "Continue with migration? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Migration cancelled"
    exit 1
fi

# Phase 1: Update Go module path
echo "Phase 1: Updating Go module path..."
find . -type f -name "*.go" -exec sed -i "s|${OLD_PATH}|${NEW_PATH}|g" {} \;
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" go.mod

# Phase 2: Update Python package
echo "Phase 2: Updating Python package..."
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" python/setup.py
sed -i "s|seyi/dagens|${seyi}/spark-ai-agents|g" python/setup.py

# Phase 3: Update documentation
echo "Phase 3: Updating documentation..."

# Fix relative paths in python/README.md
sed -i 's|../../docs/|../docs/|g' python/README.md

# Update clone instructions
sed -i "s|git clone https://github.com/seyi/dagens|git clone https://github.com/${seyi}/spark-ai-agents|g" README.md
sed -i "s|cd spark/spark-ai-agents|cd spark-ai-agents|g" README.md
sed -i "s|github.com/seyi/dagens|${NEW_PATH}|g" README.md

# Update GETTING_STARTED.md
sed -i "s|git clone https://github.com/seyi/dagens|git clone https://github.com/${seyi}/spark-ai-agents|g" docs/GETTING_STARTED.md
sed -i "s|cd spark/spark-ai-agents|cd spark-ai-agents|g" docs/GETTING_STARTED.md
sed -i "s|github.com/seyi/dagens|${NEW_PATH}|g" docs/GETTING_STARTED.md

# Update citation in README.md
sed -i "s|url = {https://github.com/seyi/dagens}|url = {https://github.com/${seyi}/spark-ai-agents}|g" README.md

# Phase 4: Clean up and test
echo "Phase 4: Verifying changes..."
go mod tidy

# Phase 5: Run tests
echo "Phase 5: Running tests..."
if go test ./... -v > /tmp/migration-test.log 2>&1; then
    echo "✅ All tests passed"
else
    echo "⚠️  Some tests failed - check /tmp/migration-test.log"
fi

echo ""
echo "=========================================="
echo "Migration complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Review changes: git diff"
echo "2. Commit: git add . && git commit -m 'Migrate to independent repository'"
echo "3. Add new remote: git remote add origin https://github.com/${seyi}/spark-ai-agents"
echo "4. Push: git push -u origin main"
echo ""
echo "Verification:"
echo "  go build ./..."
echo "  go test ./..."
echo "  cd python && pip install -e ."
```

## Manual Migration Steps

If you prefer manual migration:

### Step 1: Extract Subdirectory with History

```bash
# Clone the full Spark repo
git clone https://github.com/seyi/dagens
cd spark

# Create a standalone branch with only spark-ai-agents history
git subtree split -P spark-ai-agents -b spark-ai-agents-standalone

# Create new directory and initialize
cd ..
mkdir spark-ai-agents-new
cd spark-ai-agents-new
git init
git pull ../spark spark-ai-agents-standalone
```

### Step 2: Update Module Paths

```bash
# Update go.mod
sed -i 's|github.com/seyi/dagens|github.com/seyi/spark-ai-agents|g' go.mod

# Update all Go files
find . -type f -name "*.go" -exec sed -i 's|github.com/seyi/dagens|github.com/seyi/spark-ai-agents|g' {} \;
```

### Step 3: Update Documentation

```bash
# Update README
sed -i 's|github.com/seyi/dagens|github.com/seyi/spark-ai-agents|g' README.md
sed -i 's|cd spark/spark-ai-agents|cd spark-ai-agents|g' README.md

# Update Getting Started
sed -i 's|github.com/seyi/dagens|github.com/seyi/spark-ai-agents|g' docs/GETTING_STARTED.md

# Fix relative links
sed -i 's|../../docs/|../docs/|g' python/README.md
```

### Step 4: Update Python Package

```bash
# Update setup.py
sed -i 's|github.com/seyi/dagens|github.com/seyi/spark-ai-agents|g' python/setup.py
```

### Step 5: Verify and Test

```bash
# Tidy dependencies
go mod tidy

# Run tests
go test ./...

# Build examples
go build ./examples/...

# Test Python package
cd python
pip install -e .
cd ..
```

### Step 6: Publish

```bash
# Add remote
git remote add origin https://github.com/seyi/spark-ai-agents

# Commit changes
git add .
git commit -m "Migrate from seyi/dagens to independent repository

- Update module path from github.com/seyi/dagens
- Update all import statements
- Fix documentation links
- Update Python package metadata"

# Push
git push -u origin main

# Create initial release
git tag v0.1.0
git push origin v0.1.0
```

## For Existing Users

### Migration Guide for Users

Create a `MIGRATION_FOR_USERS.md`:

```markdown
# Migration Guide for Users

The spark-ai-agents project has moved to an independent repository.

## Old Location
github.com/seyi/dagens

## New Location
github.com/seyi/spark-ai-agents

## Update Your Code

### Go Projects

Update your imports:
```go
// Old
import "github.com/seyi/dagens/pkg/agent"

// New
import "github.com/seyi/spark-ai-agents/pkg/agent"
```

Update go.mod:
```bash
go get github.com/seyi/spark-ai-agents@latest
go mod tidy
```

### Python Projects

Update your requirements:
```bash
# Old
pip install spark-ai-agents

# New (same package name, new source)
pip install spark-ai-agents
```

## Why the Move?

- Independent release cycles
- Clearer project identity
- Simplified contributions
- Faster development
```

## Post-Migration Checklist

- [ ] All tests pass (`go test ./...`)
- [ ] Examples compile (`go build ./examples/...`)
- [ ] Python package installs (`pip install -e ./python`)
- [ ] Documentation links work
- [ ] CI/CD configured for new repo
- [ ] GitHub releases configured
- [ ] PyPI publication (if applicable)
- [ ] Update any external references
- [ ] Deprecation notice in old location

## Rollback Plan

If migration encounters issues:

```bash
# Restore from backup
git reset --hard <commit-before-migration>

# Or restore original remote
git remote remove origin
git remote add origin https://github.com/seyi/dagens
```

## Support

For migration issues:
- Open an issue at github.com/seyi/spark-ai-agents/issues
- Tag with `migration` label

## Timeline

Recommended migration timeline:
- **Week 1**: Prepare new repository, run migration script
- **Week 2**: Test thoroughly, update documentation
- **Week 3**: Publish to new location
- **Week 4+**: Monitor for issues, help users migrate
