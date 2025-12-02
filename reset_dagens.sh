#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================"
echo "Reset dagens Repository and Push Migrated Code"
echo "================================================"
echo ""
echo -e "${RED}⚠️  WARNING: This will PERMANENTLY DELETE all content and history${NC}"
echo -e "${RED}    in github.com/seyi/dagens and replace it with migrated code${NC}"
echo ""
read -p "Type 'YES I UNDERSTAND' to continue: " confirmation

if [ "$confirmation" != "YES I UNDERSTAND" ]; then
    echo "Aborted. No changes made."
    exit 1
fi

# Configuration
SPARK_AI_AGENTS_DIR="$(pwd)/spark-ai-agents"
TEMP_DIR="/tmp/dagens-reset-$(date +%s)"
REPO_URL="https://github.com/seyi/dagens.git"

# Validate spark-ai-agents directory exists
if [ ! -d "$SPARK_AI_AGENTS_DIR" ]; then
    echo -e "${RED}Error: spark-ai-agents directory not found at $SPARK_AI_AGENTS_DIR${NC}"
    echo "Please run this script from the spark repository root"
    exit 1
fi

echo ""
echo -e "${YELLOW}Step 1: Creating temporary directory...${NC}"
mkdir -p "$TEMP_DIR"
cd "$TEMP_DIR"

echo -e "${YELLOW}Step 2: Initializing fresh git repository...${NC}"
git init
git branch -M main

echo -e "${YELLOW}Step 3: Copying migrated spark-ai-agents content...${NC}"
# Copy all content
cp -r "$SPARK_AI_AGENTS_DIR"/* .
cp "$SPARK_AI_AGENTS_DIR"/.gitignore . 2>/dev/null || true
cp -r "$SPARK_AI_AGENTS_DIR"/.github . 2>/dev/null || true

# Verify go.mod has correct module path
if ! grep -q "module github.com/seyi/dagens" go.mod 2>/dev/null; then
    echo -e "${RED}Error: go.mod doesn't contain 'github.com/seyi/dagens' module path${NC}"
    echo "Please ensure migration was completed successfully"
    exit 1
fi

echo -e "${GREEN}✓ Content copied successfully${NC}"
echo ""
echo "Contents:"
ls -la | head -20

echo ""
echo -e "${YELLOW}Step 4: Creating initial commit...${NC}"
git add .
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

echo -e "${GREEN}✓ Commit created${NC}"

echo ""
echo -e "${YELLOW}Step 5: Adding remote repository...${NC}"
git remote add origin "$REPO_URL"

echo ""
echo -e "${RED}Step 6: Force pushing to dagens (DESTRUCTIVE!)...${NC}"
echo "This will replace all content in github.com/seyi/dagens"
echo ""
read -p "Press ENTER to continue or Ctrl+C to abort..."

# Try push with retry logic for network issues
MAX_RETRIES=4
RETRY_DELAY=2

for i in $(seq 1 $MAX_RETRIES); do
    echo "Push attempt $i/$MAX_RETRIES..."

    if git push -u origin main --force; then
        echo -e "${GREEN}✓ Successfully pushed to dagens!${NC}"
        break
    else
        if [ $i -eq $MAX_RETRIES ]; then
            echo -e "${RED}Error: Failed to push after $MAX_RETRIES attempts${NC}"
            echo ""
            echo "Manual push required:"
            echo "  cd $TEMP_DIR"
            echo "  git push -u origin main --force"
            exit 1
        fi

        echo "Push failed, retrying in ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
        RETRY_DELAY=$((RETRY_DELAY * 2))
    fi
done

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}✓ Success! Dagens repository has been reset${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Repository: https://github.com/seyi/dagens"
echo "Module: github.com/seyi/dagens"
echo ""
echo "Next steps:"
echo "1. Visit https://github.com/seyi/dagens to verify"
echo "2. Test: git clone https://github.com/seyi/dagens.git"
echo "3. Build: cd dagens && go build ./..."
echo "4. Run examples: go run examples/production_hardening_example.go"
echo ""
echo "Temporary directory: $TEMP_DIR"
echo "(You can delete it after verification)"
