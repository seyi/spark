#!/usr/bin/env python3
"""
Quick test script for agent HTTP server.
"""

import requests
import json

# Test 1: Health check
print("=" * 60)
print("Test 1: Health Check")
print("=" * 60)
response = requests.get("http://localhost:8080/health")
print(f"Status: {response.status_code}")
print(f"Response: {response.json()}")
print()

# Test 2: List agents
print("=" * 60)
print("Test 2: List Agents")
print("=" * 60)
response = requests.get("http://localhost:8080/api/v1/agents")
print(f"Status: {response.status_code}")
agents = response.json()["agents"]
print(f"Found {len(agents)} agents:")
for agent in agents:
    print(f"  - {agent['id']}: {agent['description']}")
print()

# Test 3: Execute echo agent
print("=" * 60)
print("Test 3: Execute Echo Agent")
print("=" * 60)
payload = {
    "agent_id": "echo",
    "input": "Hello from Python!",
    "context": {"test": "value"}
}
response = requests.post(
    "http://localhost:8080/api/v1/agents/execute",
    json=payload
)
print(f"Status: {response.status_code}")
result = response.json()
print(f"Success: {result['success']}")
print(f"Output: {result['output']}")
print(f"Duration: {result['duration_ms']}ms")
print(f"Metadata: {result['metadata']}")
print()

# Test 4: Execute summarizer agent
print("=" * 60)
print("Test 4: Execute Summarizer Agent")
print("=" * 60)
payload = {
    "agent_id": "summarizer",
    "input": "This is a test sentence with multiple words to summarize"
}
response = requests.post(
    "http://localhost:8080/api/v1/agents/execute",
    json=payload
)
result = response.json()
print(f"Success: {result['success']}")
print(f"Output: {result['output']}")
print(f"Metadata: {result['metadata']}")
print()

# Test 5: Execute classifier agent
print("=" * 60)
print("Test 5: Execute Classifier Agent")
print("=" * 60)
test_texts = [
    "This is great and excellent!",
    "This is bad and terrible!",
    "This is okay"
]
for text in test_texts:
    payload = {
        "agent_id": "classifier",
        "input": text
    }
    response = requests.post(
        "http://localhost:8080/api/v1/agents/execute",
        json=payload
    )
    result = response.json()
    print(f"Input: {text}")
    print(f"  Category: {result['output']}")
    print(f"  Confidence: {result['metadata'].get('confidence')}")
print()

# Test 6: Batch execute
print("=" * 60)
print("Test 6: Batch Execute")
print("=" * 60)
payload = {
    "agent_id": "sentiment",
    "inputs": [
        "I love this product, it's amazing!",
        "This is terrible, I hate it!",
        "It's okay, nothing special"
    ]
}
response = requests.post(
    "http://localhost:8080/api/v1/agents/batch_execute",
    json=payload
)
result = response.json()
print(f"Processed {len(result['results'])} inputs:")
for i, r in enumerate(result['results']):
    print(f"  [{i+1}] Sentiment: {r['output']}, Score: {r['metadata'].get('score')}")
print()

print("=" * 60)
print("✅ All tests completed successfully!")
print("=" * 60)
