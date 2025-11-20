package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/agent"
)

// RouterAgent dynamically routes inputs to different agents based on routing logic
// Enables intent-based routing, load balancing, and A/B testing
type RouterAgent struct {
	*agent.BaseAgent
	routes       []Route
	defaultAgent agent.Agent
	fallback     FallbackMode
	timeout      time.Duration
}

// Route defines a routing rule
type Route struct {
	Name      string
	Condition RouteCondition
	Agent     agent.Agent
	Priority  int // Higher priority routes checked first
}

// RouteCondition determines if a route should be taken
type RouteCondition func(input *agent.AgentInput) bool

// FallbackMode defines behavior when no route matches
type FallbackMode string

const (
	// FallbackError returns error if no route matches
	FallbackError FallbackMode = "error"

	// FallbackDefault uses default agent
	FallbackDefault FallbackMode = "default"

	// FallbackFirst uses first agent in routes
	FallbackFirst FallbackMode = "first"
)

// RouterAgentConfig configures a router agent
type RouterAgentConfig struct {
	Name         string
	Routes       []Route
	DefaultAgent agent.Agent
	Fallback     FallbackMode
	Timeout      time.Duration
	Dependencies []agent.Agent
}

// NewRouterAgent creates a new router agent
func NewRouterAgent(config RouterAgentConfig) *RouterAgent {
	if config.Fallback == "" {
		config.Fallback = FallbackError
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}

	// Sort routes by priority (descending)
	routes := make([]Route, len(config.Routes))
	copy(routes, config.Routes)
	for i := 0; i < len(routes)-1; i++ {
		for j := i + 1; j < len(routes); j++ {
			if routes[j].Priority > routes[i].Priority {
				routes[i], routes[j] = routes[j], routes[i]
			}
		}
	}

	routerAgent := &RouterAgent{
		routes:       routes,
		defaultAgent: config.DefaultAgent,
		fallback:     config.Fallback,
		timeout:      config.Timeout,
	}

	executor := &routerExecutor{
		routerAgent: routerAgent,
	}

	baseAgent := agent.NewAgent(agent.AgentConfig{
		Name:         config.Name,
		Executor:     executor,
		Dependencies: config.Dependencies,
	})

	routerAgent.BaseAgent = baseAgent
	return routerAgent
}

// routerExecutor implements AgentExecutor for routing
type routerExecutor struct {
	routerAgent *RouterAgent
}

func (e *routerExecutor) Execute(ctx context.Context, input *agent.AgentInput) (*agent.AgentOutput, error) {
	startTime := time.Now()

	// Create timeout context
	ctx, cancel := context.WithTimeout(ctx, e.routerAgent.timeout)
	defer cancel()

	// Find matching route
	var selectedRoute *Route
	for i := range e.routerAgent.routes {
		route := &e.routerAgent.routes[i]
		if route.Condition(input) {
			selectedRoute = route
			break
		}
	}

	// Handle no match
	if selectedRoute == nil {
		return e.handleNoMatch(ctx, input)
	}

	// Execute selected agent
	output, err := selectedRoute.Agent.Execute(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("route '%s' failed: %w", selectedRoute.Name, err)
	}

	// Add routing metadata
	if output.Metadata == nil {
		output.Metadata = make(map[string]interface{})
	}
	output.Metadata["pattern"] = "Router"
	output.Metadata["route_name"] = selectedRoute.Name
	output.Metadata["route_priority"] = selectedRoute.Priority
	output.Metadata["routing_time"] = time.Since(startTime).Seconds()

	return output, nil
}

// handleNoMatch handles case when no route matches
func (e *routerExecutor) handleNoMatch(ctx context.Context, input *agent.AgentInput) (*agent.AgentOutput, error) {
	switch e.routerAgent.fallback {
	case FallbackDefault:
		if e.routerAgent.defaultAgent == nil {
			return nil, fmt.Errorf("no route matched and no default agent configured")
		}
		return e.routerAgent.defaultAgent.Execute(ctx, input)

	case FallbackFirst:
		if len(e.routerAgent.routes) == 0 {
			return nil, fmt.Errorf("no routes configured")
		}
		return e.routerAgent.routes[0].Agent.Execute(ctx, input)

	case FallbackError:
		fallthrough
	default:
		return nil, fmt.Errorf("no route matched input")
	}
}

// RouterBuilder provides fluent API for building router agents
type RouterBuilder struct {
	name         string
	routes       []Route
	defaultAgent agent.Agent
	fallback     FallbackMode
	timeout      time.Duration
	dependencies []agent.Agent
}

// NewRouter creates a new router agent builder
func NewRouter() *RouterBuilder {
	return &RouterBuilder{
		routes:   []Route{},
		fallback: FallbackError,
		timeout:  5 * time.Minute,
	}
}

// WithName sets the agent name
func (b *RouterBuilder) WithName(name string) *RouterBuilder {
	b.name = name
	return b
}

// AddRoute adds a routing rule
func (b *RouterBuilder) AddRoute(name string, condition RouteCondition, agent agent.Agent) *RouterBuilder {
	b.routes = append(b.routes, Route{
		Name:      name,
		Condition: condition,
		Agent:     agent,
		Priority:  0,
	})
	return b
}

// AddRouteWithPriority adds a routing rule with priority
func (b *RouterBuilder) AddRouteWithPriority(name string, condition RouteCondition, agent agent.Agent, priority int) *RouterBuilder {
	b.routes = append(b.routes, Route{
		Name:      name,
		Condition: condition,
		Agent:     agent,
		Priority:  priority,
	})
	return b
}

// WithDefault sets the default agent
func (b *RouterBuilder) WithDefault(agent agent.Agent) *RouterBuilder {
	b.defaultAgent = agent
	b.fallback = FallbackDefault
	return b
}

// WithFallback sets the fallback mode
func (b *RouterBuilder) WithFallback(mode FallbackMode) *RouterBuilder {
	b.fallback = mode
	return b
}

// WithTimeout sets execution timeout
func (b *RouterBuilder) WithTimeout(timeout time.Duration) *RouterBuilder {
	b.timeout = timeout
	return b
}

// WithDependencies adds dependencies
func (b *RouterBuilder) WithDependencies(deps ...agent.Agent) *RouterBuilder {
	b.dependencies = append(b.dependencies, deps...)
	return b
}

// Build creates the router agent
func (b *RouterBuilder) Build() *RouterAgent {
	if b.name == "" {
		b.name = "router-agent"
	}

	return NewRouterAgent(RouterAgentConfig{
		Name:         b.name,
		Routes:       b.routes,
		DefaultAgent: b.defaultAgent,
		Fallback:     b.fallback,
		Timeout:      b.timeout,
		Dependencies: b.dependencies,
	})
}

// Common routing conditions

// KeywordCondition routes based on keywords in instruction
func KeywordCondition(keywords ...string) RouteCondition {
	return func(input *agent.AgentInput) bool {
		instruction := input.Instruction
		for _, keyword := range keywords {
			if contains(instruction, keyword) {
				return true
			}
		}
		return false
	}
}

// ContextKeyCondition routes based on context key value
func ContextKeyCondition(key string, value interface{}) RouteCondition {
	return func(input *agent.AgentInput) bool {
		if input.Context == nil {
			return false
		}
		val, exists := input.Context[key]
		return exists && val == value
	}
}

// ContextExistsCondition routes if context key exists
func ContextExistsCondition(key string) RouteCondition {
	return func(input *agent.AgentInput) bool {
		if input.Context == nil {
			return false
		}
		_, exists := input.Context[key]
		return exists
	}
}

// PrefixCondition routes based on instruction prefix
func PrefixCondition(prefix string) RouteCondition {
	return func(input *agent.AgentInput) bool {
		return len(input.Instruction) >= len(prefix) &&
			input.Instruction[:len(prefix)] == prefix
	}
}

// AlwaysCondition always matches
func AlwaysCondition() RouteCondition {
	return func(input *agent.AgentInput) bool {
		return true
	}
}

// NeverCondition never matches
func NeverCondition() RouteCondition {
	return func(input *agent.AgentInput) bool {
		return false
	}
}

// AndCondition combines conditions with AND logic
func AndCondition(conditions ...RouteCondition) RouteCondition {
	return func(input *agent.AgentInput) bool {
		for _, condition := range conditions {
			if !condition(input) {
				return false
			}
		}
		return true
	}
}

// OrCondition combines conditions with OR logic
func OrCondition(conditions ...RouteCondition) RouteCondition {
	return func(input *agent.AgentInput) bool {
		for _, condition := range conditions {
			if condition(input) {
				return true
			}
		}
		return false
	}
}

// NotCondition inverts a condition
func NotCondition(condition RouteCondition) RouteCondition {
	return func(input *agent.AgentInput) bool {
		return !condition(input)
	}
}

// LoadBalancerAgent distributes requests across agents using various strategies
type LoadBalancerAgent struct {
	*agent.BaseAgent
	agents   []agent.Agent
	strategy LoadBalanceStrategy
	counter  int
}

// LoadBalanceStrategy defines load balancing method
type LoadBalanceStrategy string

const (
	// RoundRobin distributes requests in round-robin fashion
	RoundRobin LoadBalanceStrategy = "round-robin"

	// Random selects agent randomly
	Random LoadBalanceStrategy = "random"

	// LeastLoaded selects agent with least load (requires metrics)
	LeastLoaded LoadBalanceStrategy = "least-loaded"
)

// LoadBalancerAgentConfig configures a load balancer agent
type LoadBalancerAgentConfig struct {
	Name         string
	Agents       []agent.Agent
	Strategy     LoadBalanceStrategy
	Dependencies []agent.Agent
}

// NewLoadBalancerAgent creates a load balancer agent
func NewLoadBalancerAgent(config LoadBalancerAgentConfig) *LoadBalancerAgent {
	if config.Strategy == "" {
		config.Strategy = RoundRobin
	}

	lbAgent := &LoadBalancerAgent{
		agents:   config.Agents,
		strategy: config.Strategy,
		counter:  0,
	}

	executor := &loadBalancerExecutor{
		lbAgent: lbAgent,
	}

	baseAgent := agent.NewAgent(agent.AgentConfig{
		Name:         config.Name,
		Executor:     executor,
		Dependencies: config.Dependencies,
	})

	lbAgent.BaseAgent = baseAgent
	return lbAgent
}

// loadBalancerExecutor implements AgentExecutor for load balancing
type loadBalancerExecutor struct {
	lbAgent *LoadBalancerAgent
}

func (e *loadBalancerExecutor) Execute(ctx context.Context, input *agent.AgentInput) (*agent.AgentOutput, error) {
	if len(e.lbAgent.agents) == 0 {
		return nil, fmt.Errorf("no agents configured")
	}

	// Select agent based on strategy
	var selectedAgent agent.Agent

	switch e.lbAgent.strategy {
	case RoundRobin:
		idx := e.lbAgent.counter % len(e.lbAgent.agents)
		e.lbAgent.counter++
		selectedAgent = e.lbAgent.agents[idx]

	case Random:
		idx := time.Now().UnixNano() % int64(len(e.lbAgent.agents))
		selectedAgent = e.lbAgent.agents[idx]

	case LeastLoaded:
		// TODO: Implement based on metrics
		// For now, fall back to round-robin
		idx := e.lbAgent.counter % len(e.lbAgent.agents)
		e.lbAgent.counter++
		selectedAgent = e.lbAgent.agents[idx]

	default:
		return nil, fmt.Errorf("unknown load balance strategy: %s", e.lbAgent.strategy)
	}

	// Execute selected agent
	output, err := selectedAgent.Execute(ctx, input)
	if err != nil {
		return nil, err
	}

	// Add load balancing metadata
	if output.Metadata == nil {
		output.Metadata = make(map[string]interface{})
	}
	output.Metadata["pattern"] = "LoadBalancer"
	output.Metadata["strategy"] = e.lbAgent.strategy
	output.Metadata["selected_agent"] = selectedAgent.Name()

	return output, nil
}

// IntentRouter routes based on detected intent
type IntentRouter struct {
	*RouterAgent
	classifier agent.Agent // Agent that classifies intent
}

// IntentRouterConfig configures intent-based routing
type IntentRouterConfig struct {
	Name            string
	Classifier      agent.Agent            // Classifies intent
	IntentToAgent   map[string]agent.Agent // Maps intent to agent
	DefaultAgent    agent.Agent
	ClassifyTimeout time.Duration
	Dependencies    []agent.Agent
}

// NewIntentRouter creates an intent-based router
func NewIntentRouter(config IntentRouterConfig) *IntentRouter {
	if config.ClassifyTimeout == 0 {
		config.ClassifyTimeout = 10 * time.Second
	}

	// Build routes from intent map
	var routes []Route
	for intent, intentAgent := range config.IntentToAgent {
		// Capture intent in closure
		intentCopy := intent
		routes = append(routes, Route{
			Name: fmt.Sprintf("intent-%s", intent),
			Condition: func(input *agent.AgentInput) bool {
				// This will be evaluated after classification
				if classifiedIntent, ok := input.Context["intent"].(string); ok {
					return classifiedIntent == intentCopy
				}
				return false
			},
			Agent:    intentAgent,
			Priority: 0,
		})
	}

	routerAgent := NewRouterAgent(RouterAgentConfig{
		Name:         config.Name,
		Routes:       routes,
		DefaultAgent: config.DefaultAgent,
		Fallback:     FallbackDefault,
		Dependencies: config.Dependencies,
	})

	return &IntentRouter{
		RouterAgent: routerAgent,
		classifier:  config.Classifier,
	}
}

// Execute classifies intent then routes
func (r *IntentRouter) Execute(ctx context.Context, input *agent.AgentInput) (*agent.AgentOutput, error) {
	// First, classify intent
	classifyOutput, err := r.classifier.Execute(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("intent classification failed: %w", err)
	}

	// Extract intent from classification result
	intent, ok := classifyOutput.Result.(string)
	if !ok {
		return nil, fmt.Errorf("classifier did not return string intent")
	}

	// Add intent to input context
	if input.Context == nil {
		input.Context = make(map[string]interface{})
	}
	input.Context["intent"] = intent

	// Route based on intent
	return r.RouterAgent.Execute(ctx, input)
}
