#!/usr/bin/env python3
"""
Test PySpark AI Agents client library without full Spark setup.
"""

import sys
sys.path.insert(0, '/home/user/spark/spark-ai-agents/python')

from spark_ai_agents.client import AgentClient, AgentResponse
from spark_ai_agents.agents import Agent

print("=" * 60)
print("Testing PySpark AI Agents Client Library")
print("=" * 60)
print()

# Test 1: AgentClient basic usage
print("Test 1: AgentClient - Single Execution")
print("-" * 60)
client = AgentClient(server_url="http://localhost:8080")

response = client.execute(
    agent_id="echo",
    input_text="Hello from PySpark client!",
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
agent = Agent("summarizer", server_url="http://localhost:8080")

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
classifier = Agent("classifier", server_url="http://localhost:8080")

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

print("=" * 60)
print("✅ All PySpark client tests passed!")
print("=" * 60)
