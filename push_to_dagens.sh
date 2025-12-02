#!/bin/bash
set -e

echo "================================================"
echo "Push Dagens AI Agents to GitHub"
echo "================================================"
echo ""

# Check if we're in the right directory
if [ ! -d "spark-ai-agents" ]; then
    echo "Error: spark-ai-agents directory not found"
    echo "Please run this script from the spark repository root"
    exit 1
fi

DAGENS_DIR="${1:-/tmp/dagens-push}"

echo "Creating temporary directory: $DAGENS_DIR"
mkdir -p $DAGENS_DIR
cd $DAGENS_DIR

# Initialize git if needed
if [ ! -d ".git" ]; then
    echo "Initializing git repository..."
    git init
    git branch -M main
fi

# Copy all migrated content
echo "Copying migrated content from spark-ai-agents..."
cd -
cp -r spark-ai-agents/* $DAGENS_DIR/
cp spark-ai-agents/.gitignore $DAGENS_DIR/ 2>/dev/null || true
cp -r spark-ai-agents/.github $DAGENS_DIR/ 2>/dev/null || true

cd $DAGENS_DIR

# Stage all files
echo "Staging files..."
git add .

# Commit
echo "Creating commit..."
git commit -m "Initial commit: Dagens AI Agents framework

Complete production-ready AI agent framework featuring:

- Distributed agent execution with task delegation
- Production hardening: resilience patterns, state management, observability
- Circuit breakers, retry logic, rate limiting
- Multiple state backends (memory, file, S3, Redis)
- Structured logging and Prometheus metrics
- Comprehensive examples and documentation
- Python bindings for cross-language support

Migrated from apache/spark spark-ai-agents project.
Module path: github.com/seyi/dagens
Python package: dagens"

# Add remote
echo "Adding GitHub remote..."
git remote add origin https://github.com/seyi/dagens.git 2>/dev/null || \
    git remote set-url origin https://github.com/seyi/dagens.git

# Push
echo ""
echo "Ready to push to GitHub!"
echo "Run the following command:"
echo ""
echo "  cd $DAGENS_DIR && git push -u origin main"
echo ""
echo "Or if you need to force push (careful!):"
echo "  cd $DAGENS_DIR && git push -u origin main --force"
echo ""
