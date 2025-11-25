#!/bin/bash
set -e

OLD_PATH="github.com/apache/spark/spark-ai-agents"
NEW_PATH="github.com/seyi/dagens"

echo "=========================================="
echo "Migrating to github.com/seyi/dagens"
echo "=========================================="
echo "Old path: ${OLD_PATH}"
echo "New path: ${NEW_PATH}"
echo ""

# Phase 1: Update Go module path in go.mod
echo "Phase 1: Updating go.mod..."
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" go.mod

# Phase 2: Update all Go files
echo "Phase 2: Updating Go import statements (152 files)..."
find . -type f -name "*.go" -exec sed -i "s|${OLD_PATH}|${NEW_PATH}|g" {} \;

# Phase 3: Update Python package
echo "Phase 3: Updating Python package..."
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" python/setup.py
sed -i "s|apache/spark|seyi/dagens|g" python/setup.py
sed -i "s|dev@spark.apache.org|seyi@example.com|g" python/setup.py

# Phase 4: Update documentation
echo "Phase 4: Updating documentation..."

# Fix relative paths in python/README.md
sed -i 's|../../docs/|../docs/|g' python/README.md

# Update README.md
sed -i "s|git clone https://github.com/apache/spark|git clone https://github.com/seyi/dagens|g" README.md
sed -i "s|cd spark/spark-ai-agents|cd dagens|g" README.md
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" README.md
sed -i "s|url = {https://github.com/apache/spark/spark-ai-agents}|url = {https://github.com/seyi/dagens}|g" README.md

# Update GETTING_STARTED.md
sed -i "s|git clone https://github.com/apache/spark|git clone https://github.com/seyi/dagens|g" docs/GETTING_STARTED.md
sed -i "s|cd spark/spark-ai-agents|cd dagens|g" docs/GETTING_STARTED.md
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" docs/GETTING_STARTED.md

# Update MIGRATION_GUIDE.md
sed -i "s|${OLD_PATH}|${NEW_PATH}|g" docs/MIGRATION_GUIDE.md
sed -i "s|NEW_ORG|seyi|g" docs/MIGRATION_GUIDE.md
sed -i "s|apache/spark|seyi/dagens|g" docs/MIGRATION_GUIDE.md

# Update MCP README
if [ -f "pkg/tools/mcp/README.md" ]; then
    sed -i "s|../../../examples/|../../examples/|g" pkg/tools/mcp/README.md
    sed -i "s|../../../README.md|../../README.md|g" pkg/tools/mcp/README.md
fi

# Update package name references
sed -i "s|Spark AI Agents|Dagens AI Agents|g" README.md
sed -i "s|spark-ai-agents|dagens|g" python/setup.py
sed -i 's|name="spark-ai-agents"|name="dagens-ai-agents"|g' python/setup.py
sed -i 's|pip install spark-ai-agents|pip install dagens-ai-agents|g' python/README.md README.md docs/GETTING_STARTED.md

# Phase 5: Clean up and verify
echo "Phase 5: Tidying Go modules..."
go mod tidy

echo ""
echo "=========================================="
echo "Migration Complete!"
echo "=========================================="
echo ""
echo "Changes made:"
echo "  ✓ Updated go.mod module path"
echo "  ✓ Updated 152 Go files with new import paths"
echo "  ✓ Updated Python package (setup.py)"
echo "  ✓ Updated all documentation"
echo "  ✓ Fixed relative paths"
echo "  ✓ Renamed package to dagens-ai-agents"
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff"
echo "  2. Run tests: go test ./..."
echo "  3. Build examples: go build ./examples/..."
echo "  4. Add remote: git remote add dagens https://github.com/seyi/dagens.git"
echo "  5. Commit: git add . && git commit -m 'Migrate to github.com/seyi/dagens'"
echo "  6. Push: git push dagens HEAD:main"
