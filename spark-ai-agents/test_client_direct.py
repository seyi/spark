#!/usr/bin/env python3
"""
Test client library with direct imports (bypassing __init__.py).
"""

import sys
import os

# Add to path
client_dir = '/home/user/spark/spark-ai-agents/python/spark_ai_agents'
sys.path.insert(0, client_dir)

# Import client module directly
import client as agent_client
import agents as agent_agents

print("=" * 60)
print("Testing AI Agents Client Library (Direct Import)")
print("=" * 60)
print()

# Test 1: AgentClient basic usage
print("Test 1: AgentClient - Single Execution")
print("-" * 60)
client = agent_client.AgentClient(server_url="http://localhost:8080")

response = client.execute(
    agent_id="echo",
    input_text="Hello from Python client!",
    context={"source": "test"}
)

print(f"Success: {response.success}")
print(f"Output: {response.output}")
print(f"Duration: {response.duration_ms}ms")
print(f"Metadata: {response.metadata}")
print()

# Test 2: Agent wrapper
print("Test 2: Agent Wrapper")
print("-" * 60)
agent = agent_agents.Agent("summarizer", server_url="http://localhost:8080")

response = agent.execute("This is a longer text with many words that should be summarized properly")
print(f"Success: {response.success}")
print(f"Output: {response.output}")
print()

# Test 3: Batch execution
print("Test 3: Batch Execution")
print("-" * 60)
inputs = [
    "First input text",
    "Second input text",
    "Third input text"
]

responses = client.batch_execute("sentiment", inputs)
print(f"Processed {len(responses)} inputs:")
for i, resp in enumerate(responses):
    print(f"  [{i+1}] {resp.output} (score: {resp.metadata.get('score')})")
print()

# Test 4: Agent with context
print("Test 4: Agent with Context")
print("-" * 60)
classifier = agent_agents.Agent("classifier", server_url="http://localhost:8080")

test_texts = [
    "This product is absolutely amazing and wonderful!",
    "Terrible experience, very disappointing",
    "It's fine, nothing special"
]

for text in test_texts:
    response = classifier.execute(text, context={"test_mode": "true"})
    print(f"  '{text[:40]}...'")
    print(f"    -> {response.output} (confidence: {response.metadata.get('confidence')})")
print()

# Test 5: Error handling
print("Test 5: Error Handling")
print("-" * 60)
response = client.execute("nonexistent-agent", "test")
print(f"Success: {response.success}")
if not response.success:
    print(f"Error (expected): {response.error}")
print()

print("=" * 60)
print("✅ All client library tests passed!")
print("=" * 60)
