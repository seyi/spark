# Phase 3: Advanced Features

This document describes the Phase 3 features added to Spark AI Agents, providing advanced capabilities while maintaining the distributed Spark architecture.

## Overview

Phase 3 adds four major feature sets:

1. **Planning Module** - Automated task decomposition and DAG generation
2. **Artifact Management** - Structured output handling and versioning
3. **A2A Protocol** - Standards-based agent-to-agent communication
4. **Authentication & Security** - RBAC and multi-tenant support

All features are designed to work seamlessly with the existing distributed architecture from Phases 1 & 2.

---

## 1. Planning Module

### Overview

The Planning Module provides intelligent task decomposition, converting high-level objectives into executable DAGs with dependency management and optimization.

### Key Features

- **Automated Task Decomposition**: Break complex objectives into manageable steps
- **DAG Generation**: Convert plans into executable AgentDAGs
- **Plan Validation**: Detect circular dependencies and invalid configurations
- **Plan Optimization**: Maximize parallelism and minimize execution time
- **Multiple Patterns**: Support sequential, parallel, pipeline, map-reduce, and hierarchical patterns

### Architecture

```
┌─────────────┐
│  Objective  │
└──────┬──────┘
       │
       v
┌─────────────┐      ┌────────────────┐
│   Planner   │─────>│ PlanConstraints│
└──────┬──────┘      └────────────────┘
       │
       v
┌─────────────┐
│    Plan     │
│  ├─ Step 1  │
│  ├─ Step 2  │
│  └─ Step 3  │
└──────┬──────┘
       │
       v
┌─────────────┐
│ Validate &  │
│  Optimize   │
└──────┬──────┘
       │
       v
┌─────────────┐
│  AgentDAG   │
└─────────────┘
```

### Usage Example

```go
// Create planner
planner := planner.NewDefaultPlanner(agentRegistry, modelProvider)

// Define constraints
constraints := planner.PlanConstraints{
    MaxSteps:       10,
    MaxParallelism: 4,
    Timeout:        5 * time.Minute,
    PreferredPattern: planner.PatternHierarchical,
}

// Create plan
plan, err := planner.CreatePlan(ctx, "Research AI agents and create presentation", constraints)

// Validate
if err := planner.ValidatePlan(plan); err != nil {
    log.Fatal(err)
}

// Optimize for parallel execution
optimized, err := planner.OptimizePlan(plan)

// Convert to DAG
dag, err := planner.PlanToDAG(optimized, agentRegistry)

// Execute via coordinator
coordinator.SubmitDAG(ctx, dag)
```

### Integration with Spark Architecture

- Plans are converted to AgentDAGs for distributed execution
- DAGScheduler handles stage-based execution
- TaskScheduler applies locality-aware placement
- Fault tolerance via lineage tracking

---

## 2. Artifact Management

### Overview

Artifact Management provides structured handling of agent outputs with versioning, metadata, and efficient storage.

### Key Features

- **Multiple Artifact Types**: Text, code, images, audio, video, data, JSON, markdown, PDF, archives
- **Version Control**: Automatic versioning with parent tracking
- **Checksum Validation**: SHA-256 checksums for integrity
- **Metadata & Tags**: Rich metadata and tagging system
- **Expiration Support**: Time-based artifact expiration
- **Filtering & Search**: Query by agent, session, task, type, tags, time range
- **Streaming Access**: io.Reader/io.Seeker interface for large artifacts

### Architecture

```
┌────────────────┐
│ Agent Output   │
└────────┬───────┘
         │
         v
┌────────────────┐       ┌──────────────┐
│ ArtifactStore  │<─────>│ Version DB   │
│                │       └──────────────┘
│ - Save         │
│ - Load         │       ┌──────────────┐
│ - List         │<─────>│  Index/Tags  │
│ - Delete       │       └──────────────┘
│ - GetVersion   │
└────────────────┘
```

### Usage Example

```go
// Create artifact store
store := artifacts.NewInMemoryArtifactStore()

// Create artifact using builder
artifact := artifacts.NewArtifactBuilder().
    WithType(artifacts.ArtifactTypeCode).
    WithContent([]byte("def hello(): print('hello')")).
    WithContentType("text/x-python").
    WithAgentID("code-agent").
    WithSessionID("session-123").
    WithTags("python", "function").
    WithMetadata("language", "python").
    Build()

// Save artifact
err := store.Save(ctx, artifact)

// Search artifacts
results, err := store.List(ctx, artifacts.ArtifactFilters{
    AgentID:   "code-agent",
    Type:      artifacts.ArtifactTypeCode,
    Tags:      []string{"python"},
    Limit:     10,
})

// Access specific version
v1, err := store.GetVersion(ctx, artifact.ID, 1)

// Stream large artifact
reader := artifacts.NewArtifactReader(artifact)
io.Copy(outputFile, reader)
```

### Distributed Integration

- Artifacts can be stored in distributed backends (S3, HDFS, etc.)
- Metadata indexed for fast querying
- Checkpoints can include artifact references
- Locality-aware access for large artifacts

---

## 3. A2A Protocol (Agent-to-Agent)

### Overview

The A2A Protocol implements standards-based agent-to-agent communication using JSON-RPC 2.0 over HTTP(S), based on the A2A Project specification.

### Key Features

- **Agent Cards**: Capability declarations for discovery
- **Service Discovery**: Find agents by capabilities
- **JSON-RPC 2.0**: Standard protocol over HTTP(S)
- **Multiple Patterns**: Request/response, SSE streaming, async push
- **Modalities**: Text, forms, media, streaming
- **Authentication**: Bearer, API key, mTLS support
- **Multi-Language**: Designed for interoperability

### Architecture

```
┌──────────────────┐
│  Agent Registry  │
│  (Agent Cards)   │
└────────┬─────────┘
         │
         v
┌──────────────────┐      ┌──────────────────┐
│   A2A Client     │<────>│   HTTP Client    │
│                  │      └──────────────────┘
│ - InvokeAgent    │
│ - DiscoverAgents │      ┌──────────────────┐
│ - GetAgentCard   │      │   JSON-RPC 2.0   │
│ - StreamInvoke   │      │   Protocol       │
└──────────────────┘      └──────────────────┘
```

### Agent Card Structure

```json
{
  "id": "research-agent",
  "name": "Research Assistant",
  "description": "Conducts web research and summarization",
  "version": "1.0.0",
  "endpoint": "https://api.example.com/agents/research",
  "capabilities": [
    {
      "name": "web_research",
      "description": "Search and analyze web content",
      "input_schema": {"query": "string"},
      "output_schema": {"summary": "string", "sources": "array"}
    }
  ],
  "modalities": ["text", "stream"],
  "auth_scheme": {
    "type": "bearer",
    "parameters": {}
  },
  "supported_patterns": ["request_response", "server_sent_events"]
}
```

### Usage Example

```go
// Create discovery registry
registry := a2a.NewDiscoveryRegistry()

// Register agent with card
card := &a2a.AgentCard{
    ID:          "research-agent",
    Name:        "Research Assistant",
    Description: "Conducts research",
    Endpoint:    "http://localhost:8001/invoke",
    Capabilities: []a2a.Capability{
        {
            Name:        "web_research",
            Description: "Search web content",
        },
    },
    Modalities: []a2a.Modality{a2a.ModalityText},
    AuthScheme: a2a.AuthScheme{Type: a2a.AuthTypeBearer},
}

registry.Register(card)

// Create A2A client
client := a2a.NewHTTPA2AClient(registry)

// Discover agents
agents, err := client.DiscoverAgents(ctx, "web_research")

// Invoke remote agent
output, err := client.InvokeAgent(ctx, "research-agent", &agent.AgentInput{
    Instruction: "Research AI trends",
    Context: map[string]interface{}{
        "capability": "web_research",
    },
})
```

### Distributed Integration

- Agents can be distributed across clusters
- Discovery registry supports federation
- Load balancing across agent instances
- Fault-tolerant remote invocations
- Compatible with service meshes

---

## 4. Authentication & Security

### Overview

Comprehensive authentication and authorization with RBAC (Role-Based Access Control), multi-tenancy, and audit logging.

### Key Features

- **Multiple Auth Methods**: Password, API key, bearer token, mTLS
- **RBAC**: Role-based permissions with wildcard support
- **Multi-Tenancy**: Tenant isolation and scoping
- **Session Management**: Token-based sessions with expiration
- **API Keys**: Revocable, scoped API keys
- **Audit Logging**: Comprehensive event tracking
- **Resource Wildcards**: Flexible permission patterns

### Architecture

```
┌──────────────────┐
│   Credentials    │
└────────┬─────────┘
         │
         v
┌──────────────────┐      ┌──────────────────┐
│ Authenticator    │─────>│   User Store     │
│                  │      └──────────────────┘
│ - Authenticate   │
│ - ValidateToken  │      ┌──────────────────┐
│ - RevokeToken    │─────>│  Session Store   │
└──────────────────┘      └──────────────────┘
         │
         v
┌──────────────────┐
│   Principal      │
│  (Authenticated) │
└────────┬─────────┘
         │
         v
┌──────────────────┐      ┌──────────────────┐
│   Authorizer     │─────>│ Role Permissions │
│                  │      └──────────────────┘
│ - Authorize      │
│ - GrantPerm      │      ┌──────────────────┐
│ - RevokePerm     │      │   Audit Logger   │
└──────────────────┘      └──────────────────┘
```

### Usage Example

```go
// Create authenticator and authorizer
authenticator := auth.NewDefaultAuthenticator()
authorizer := auth.NewDefaultAuthorizer()

// Register user
user := &auth.User{
    Username:     "alice",
    Email:        "alice@example.com",
    PasswordHash: auth.HashPassword("secret123"),
    TenantID:     "tenant-1",
    Roles:        []string{"agent_user", "developer"},
}
authenticator.RegisterUser(user)

// Authenticate with password
credentials := auth.Credentials{
    Type:     auth.CredentialTypePassword,
    Username: "alice",
    Password: "secret123",
}
principal, err := authenticator.Authenticate(ctx, credentials)

// Create API key
apiKey, err := authenticator.CreateAPIKey(principal.ID, "Production Key", nil)

// Set up RBAC
authorizer.GrantPermission("developer", auth.Permission{
    Resource: "agents/*",
    Actions:  []string{"read", "write", "execute", "delete"},
    Scope:    "tenant",
})

// Check authorization
allowed, err := authorizer.Authorize(principal, "agents/research-agent", "execute")

// Audit logging
auditLogger := auth.NewAuditLogger()
auditLogger.Log("agent.execute", principal.ID, "agents/research-agent", "execute", "success", nil)
```

### Security Best Practices

1. **Never store plaintext passwords** - Use strong hashing (SHA-256 minimum)
2. **Rotate API keys regularly** - Set expiration dates
3. **Use least privilege** - Grant minimum necessary permissions
4. **Enable audit logging** - Track all security events
5. **Implement rate limiting** - Prevent brute force attacks
6. **Use HTTPS/TLS** - Encrypt all network traffic
7. **Tenant isolation** - Enforce strict tenant boundaries

### Distributed Integration

- Authentication can use distributed stores (Redis, etcd)
- Authorization decisions cached for performance
- Audit logs can be sent to centralized logging
- Multi-region session replication
- Integration with existing IAM systems

---

## Integration Example

Here's how all Phase 3 features work together in a complete workflow:

```go
// 1. Authenticate user
authenticator := auth.NewDefaultAuthenticator()
principal, err := authenticator.Authenticate(ctx, credentials)

// 2. Check authorization
authorizer := auth.NewDefaultAuthorizer()
allowed, err := authorizer.Authorize(principal, "workflow/*", "execute")

// 3. Create execution plan
planner := planner.NewDefaultPlanner(agentRegistry, nil)
plan, err := planner.CreatePlan(ctx, "Research and report on AI", constraints)

// 4. Discover agents via A2A
registry := a2a.NewDiscoveryRegistry()
agents, err := a2a.NewHTTPA2AClient(registry).DiscoverAgents(ctx, "research")

// 5. Execute plan (generates DAG)
dag, err := planner.PlanToDAG(plan, agentRegistry)
jobID, err := coordinator.SubmitDAG(ctx, dag)

// 6. Store artifacts
artifactStore := artifacts.NewInMemoryArtifactStore()
artifact := /* agent output */
err = artifactStore.Save(ctx, artifact)

// 7. Audit the workflow
auditLogger.Log("workflow.complete", principal.ID, jobID, "execute", "success", metadata)
```

---

## Performance Considerations

### Planning Module
- Model-based planning can be expensive - consider caching
- Plan optimization is O(n²) in worst case - limit max steps
- DAG conversion is lightweight

### Artifact Management
- In-memory store suitable for small artifacts
- Use distributed stores (S3, HDFS) for large files
- Index metadata separately for fast queries

### A2A Protocol
- HTTP overhead - use connection pooling
- Consider gRPC for high-throughput scenarios
- Implement circuit breakers for fault tolerance

### Authentication
- Hash passwords with bcrypt in production (not SHA-256)
- Cache authorization decisions with TTL
- Use connection pooling for auth stores

---

## Testing

Run Phase 3 tests:

```bash
# All Phase 3 tests
go test ./pkg/planner/... ./pkg/artifacts/... ./pkg/a2a/... ./pkg/auth/...

# Specific package
go test ./pkg/planner -v

# With coverage
go test ./pkg/... -cover
```

---

## Next Steps

Phase 3 completes the advanced features. Next comes Phase 4 (Polish):

1. Development UI
2. Plugin Architecture
3. Comprehensive Documentation
4. Production Examples

---

## References

- [A2A Protocol Specification](https://github.com/a2aproject)
- [ADK Python](https://github.com/google/adk-python)
- [Apache Spark Architecture](https://spark.apache.org/docs/latest/)
- [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification)
- [RBAC Best Practices](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html)
