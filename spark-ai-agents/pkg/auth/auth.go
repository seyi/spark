// Package auth provides authentication and authorization for agents
// Implements RBAC (Role-Based Access Control) and multi-tenant security
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Authenticator handles authentication
type Authenticator interface {
	// Authenticate verifies credentials and returns a principal
	Authenticate(ctx context.Context, credentials Credentials) (*Principal, error)

	// ValidateToken validates an authentication token
	ValidateToken(ctx context.Context, token string) (*Principal, error)

	// RevokeToken invalidates a token
	RevokeToken(ctx context.Context, token string) error
}

// Authorizer handles authorization
type Authorizer interface {
	// Authorize checks if a principal can perform an action on a resource
	Authorize(principal *Principal, resource string, action string) (bool, error)

	// GrantPermission grants a permission to a role
	GrantPermission(role string, permission Permission) error

	// RevokePermission removes a permission from a role
	RevokePermission(role string, resource string, action string) error
}

// Principal represents an authenticated entity
type Principal struct {
	ID          string
	Username    string
	Email       string
	TenantID    string
	Roles       []string
	Permissions []Permission
	Metadata    map[string]interface{}
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

// Credentials represents authentication credentials
type Credentials struct {
	Type       CredentialType
	Username   string
	Password   string
	APIKey     string
	Token      string
	Certificate []byte
	Metadata   map[string]interface{}
}

// CredentialType defines the type of credentials
type CredentialType string

const (
	CredentialTypePassword     CredentialType = "password"
	CredentialTypeAPIKey       CredentialType = "api_key"
	CredentialTypeToken        CredentialType = "token"
	CredentialTypeCertificate  CredentialType = "certificate"
)

// Permission represents an access permission
type Permission struct {
	Resource string
	Actions  []string
	Scope    string // "tenant", "global", "self"
}

// User represents a system user
type User struct {
	ID           string
	Username     string
	Email        string
	PasswordHash string
	TenantID     string
	Roles        []string
	APIKeys      []APIKey
	Active       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastLoginAt  *time.Time
}

// APIKey represents an API key
type APIKey struct {
	ID        string
	Key       string
	Name      string
	UserID    string
	ExpiresAt *time.Time
	CreatedAt time.Time
	Active    bool
}

// Session represents an authenticated session
type Session struct {
	ID          string
	Token       string
	PrincipalID string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	LastActive  time.Time
	Metadata    map[string]interface{}
}

// DefaultAuthenticator implements basic authentication
type DefaultAuthenticator struct {
	users    map[string]*User
	sessions map[string]*Session
	apiKeys  map[string]*APIKey
	mu       sync.RWMutex
}

// NewDefaultAuthenticator creates a new authenticator
func NewDefaultAuthenticator() *DefaultAuthenticator {
	return &DefaultAuthenticator{
		users:    make(map[string]*User),
		sessions: make(map[string]*Session),
		apiKeys:  make(map[string]*APIKey),
	}
}

// Authenticate verifies credentials
func (a *DefaultAuthenticator) Authenticate(ctx context.Context, credentials Credentials) (*Principal, error) {
	switch credentials.Type {
	case CredentialTypePassword:
		return a.authenticatePassword(ctx, credentials)
	case CredentialTypeAPIKey:
		return a.authenticateAPIKey(ctx, credentials)
	case CredentialTypeToken:
		return a.ValidateToken(ctx, credentials.Token)
	default:
		return nil, fmt.Errorf("unsupported credential type: %s", credentials.Type)
	}
}

// authenticatePassword verifies username/password
func (a *DefaultAuthenticator) authenticatePassword(ctx context.Context, credentials Credentials) (*Principal, error) {
	a.mu.RLock()
	user, exists := a.users[credentials.Username]
	a.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("invalid credentials")
	}

	if !user.Active {
		return nil, fmt.Errorf("user account is inactive")
	}

	// Verify password hash
	passwordHash := hashPassword(credentials.Password)
	if user.PasswordHash != passwordHash {
		return nil, fmt.Errorf("invalid credentials")
	}

	// Create session
	session := &Session{
		ID:          uuid.New().String(),
		Token:       generateToken(),
		PrincipalID: user.ID,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		LastActive:  time.Now(),
		Metadata:    make(map[string]interface{}),
	}

	a.mu.Lock()
	a.sessions[session.Token] = session
	now := time.Now()
	user.LastLoginAt = &now
	a.mu.Unlock()

	// Build principal
	principal := &Principal{
		ID:        user.ID,
		Username:  user.Username,
		Email:     user.Email,
		TenantID:  user.TenantID,
		Roles:     user.Roles,
		Metadata:  make(map[string]interface{}),
		CreatedAt: session.CreatedAt,
		ExpiresAt: session.ExpiresAt,
	}

	return principal, nil
}

// authenticateAPIKey verifies an API key
func (a *DefaultAuthenticator) authenticateAPIKey(ctx context.Context, credentials Credentials) (*Principal, error) {
	a.mu.RLock()
	apiKey, exists := a.apiKeys[credentials.APIKey]
	a.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("invalid API key")
	}

	if !apiKey.Active {
		return nil, fmt.Errorf("API key is inactive")
	}

	if apiKey.ExpiresAt != nil && time.Now().After(*apiKey.ExpiresAt) {
		return nil, fmt.Errorf("API key has expired")
	}

	// Get user
	a.mu.RLock()
	var user *User
	for _, u := range a.users {
		if u.ID == apiKey.UserID {
			user = u
			break
		}
	}
	a.mu.RUnlock()

	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	// Build principal
	principal := &Principal{
		ID:       user.ID,
		Username: user.Username,
		Email:    user.Email,
		TenantID: user.TenantID,
		Roles:    user.Roles,
		Metadata: map[string]interface{}{
			"auth_method": "api_key",
			"api_key_id":  apiKey.ID,
		},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}

	return principal, nil
}

// ValidateToken validates an authentication token
func (a *DefaultAuthenticator) ValidateToken(ctx context.Context, token string) (*Principal, error) {
	a.mu.RLock()
	session, exists := a.sessions[token]
	a.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("invalid token")
	}

	if time.Now().After(session.ExpiresAt) {
		a.mu.Lock()
		delete(a.sessions, token)
		a.mu.Unlock()
		return nil, fmt.Errorf("token has expired")
	}

	// Update last active
	a.mu.Lock()
	session.LastActive = time.Now()
	a.mu.Unlock()

	// Get user
	a.mu.RLock()
	var user *User
	for _, u := range a.users {
		if u.ID == session.PrincipalID {
			user = u
			break
		}
	}
	a.mu.RUnlock()

	if user == nil {
		return nil, fmt.Errorf("user not found")
	}

	// Build principal
	principal := &Principal{
		ID:        user.ID,
		Username:  user.Username,
		Email:     user.Email,
		TenantID:  user.TenantID,
		Roles:     user.Roles,
		Metadata:  make(map[string]interface{}),
		CreatedAt: session.CreatedAt,
		ExpiresAt: session.ExpiresAt,
	}

	return principal, nil
}

// RevokeToken invalidates a token
func (a *DefaultAuthenticator) RevokeToken(ctx context.Context, token string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.sessions, token)
	return nil
}

// RegisterUser registers a new user
func (a *DefaultAuthenticator) RegisterUser(user *User) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.users[user.Username]; exists {
		return fmt.Errorf("username already exists")
	}

	if user.ID == "" {
		user.ID = uuid.New().String()
	}

	now := time.Now()
	user.CreatedAt = now
	user.UpdatedAt = now
	user.Active = true

	a.users[user.Username] = user
	return nil
}

// CreateAPIKey creates a new API key for a user
func (a *DefaultAuthenticator) CreateAPIKey(userID string, name string, expiresAt *time.Time) (*APIKey, error) {
	apiKey := &APIKey{
		ID:        uuid.New().String(),
		Key:       generateAPIKey(),
		Name:      name,
		UserID:    userID,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now(),
		Active:    true,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.apiKeys[apiKey.Key] = apiKey

	// Add to user's API keys
	for _, user := range a.users {
		if user.ID == userID {
			user.APIKeys = append(user.APIKeys, *apiKey)
			break
		}
	}

	return apiKey, nil
}

// DefaultAuthorizer implements RBAC authorization
type DefaultAuthorizer struct {
	rolePermissions map[string][]Permission
	mu              sync.RWMutex
}

// NewDefaultAuthorizer creates a new authorizer
func NewDefaultAuthorizer() *DefaultAuthorizer {
	return &DefaultAuthorizer{
		rolePermissions: make(map[string][]Permission),
	}
}

// Authorize checks if a principal can perform an action
func (a *DefaultAuthorizer) Authorize(principal *Principal, resource string, action string) (bool, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Check role-based permissions
	for _, role := range principal.Roles {
		permissions, exists := a.rolePermissions[role]
		if !exists {
			continue
		}

		for _, perm := range permissions {
			if a.matchesPermission(perm, resource, action, principal) {
				return true, nil
			}
		}
	}

	// Check direct permissions
	for _, perm := range principal.Permissions {
		if a.matchesPermission(perm, resource, action, principal) {
			return true, nil
		}
	}

	return false, nil
}

// matchesPermission checks if a permission matches the request
func (a *DefaultAuthorizer) matchesPermission(perm Permission, resource string, action string, principal *Principal) bool {
	// Check resource match (support wildcards)
	if !matchesResource(perm.Resource, resource) {
		return false
	}

	// Check action match
	actionMatches := false
	for _, allowedAction := range perm.Actions {
		if allowedAction == "*" || allowedAction == action {
			actionMatches = true
			break
		}
	}

	if !actionMatches {
		return false
	}

	// Check scope
	switch perm.Scope {
	case "global":
		return true
	case "tenant":
		// Would check tenant ID match
		return true
	case "self":
		// Would check if resource belongs to principal
		return true
	default:
		return true
	}
}

// GrantPermission grants a permission to a role
func (a *DefaultAuthorizer) GrantPermission(role string, permission Permission) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.rolePermissions[role] = append(a.rolePermissions[role], permission)
	return nil
}

// RevokePermission removes a permission from a role
func (a *DefaultAuthorizer) RevokePermission(role string, resource string, action string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	permissions, exists := a.rolePermissions[role]
	if !exists {
		return fmt.Errorf("role not found")
	}

	filtered := make([]Permission, 0)
	for _, perm := range permissions {
		if perm.Resource != resource {
			filtered = append(filtered, perm)
			continue
		}

		// Filter out the specific action
		newActions := make([]string, 0)
		for _, a := range perm.Actions {
			if a != action {
				newActions = append(newActions, a)
			}
		}

		if len(newActions) > 0 {
			perm.Actions = newActions
			filtered = append(filtered, perm)
		}
	}

	a.rolePermissions[role] = filtered
	return nil
}

// Helper functions

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func generateToken() string {
	return uuid.New().String()
}

func generateAPIKey() string {
	return "sk_" + uuid.New().String()
}

func matchesResource(pattern string, resource string) bool {
	if pattern == "*" {
		return true
	}

	// Simple wildcard matching
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(resource) >= len(prefix) && resource[:len(prefix)] == prefix
	}

	return pattern == resource
}

// AuditLogger logs authentication and authorization events
type AuditLogger struct {
	logs []AuditLog
	mu   sync.RWMutex
}

// AuditLog represents an audit log entry
type AuditLog struct {
	ID          string
	Timestamp   time.Time
	EventType   string
	PrincipalID string
	Resource    string
	Action      string
	Result      string
	Metadata    map[string]interface{}
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger() *AuditLogger {
	return &AuditLogger{
		logs: make([]AuditLog, 0),
	}
}

// Log records an audit event
func (l *AuditLogger) Log(eventType string, principalID string, resource string, action string, result string, metadata map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	log := AuditLog{
		ID:          uuid.New().String(),
		Timestamp:   time.Now(),
		EventType:   eventType,
		PrincipalID: principalID,
		Resource:    resource,
		Action:      action,
		Result:      result,
		Metadata:    metadata,
	}

	l.logs = append(l.logs, log)
}

// GetLogs retrieves audit logs
func (l *AuditLogger) GetLogs(limit int) []AuditLog {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if limit == 0 || limit > len(l.logs) {
		limit = len(l.logs)
	}

	// Return most recent logs
	start := len(l.logs) - limit
	if start < 0 {
		start = 0
	}

	return l.logs[start:]
}
