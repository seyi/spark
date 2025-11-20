package auth

import (
	"context"
	"testing"
	"time"
)

func TestNewDefaultAuthenticator(t *testing.T) {
	auth := NewDefaultAuthenticator()

	if auth == nil {
		t.Fatal("Authenticator should not be nil")
	}
}

func TestRegisterUser(t *testing.T) {
	auth := NewDefaultAuthenticator()

	user := &User{
		Username:     "alice",
		Email:        "alice@example.com",
		PasswordHash: hashPassword("password123"),
		TenantID:     "tenant-1",
		Roles:        []string{"user"},
	}

	err := auth.RegisterUser(user)
	if err != nil {
		t.Fatalf("Failed to register user: %v", err)
	}

	if user.ID == "" {
		t.Error("User ID should be generated")
	}

	if !user.Active {
		t.Error("New user should be active")
	}
}

func TestAuthenticatePassword(t *testing.T) {
	auth := NewDefaultAuthenticator()
	ctx := context.Background()

	// Register user
	user := &User{
		Username:     "alice",
		Email:        "alice@example.com",
		PasswordHash: hashPassword("password123"),
		TenantID:     "tenant-1",
		Roles:        []string{"user"},
	}
	auth.RegisterUser(user)

	// Authenticate with correct password
	credentials := Credentials{
		Type:     CredentialTypePassword,
		Username: "alice",
		Password: "password123",
	}

	principal, err := auth.Authenticate(ctx, credentials)
	if err != nil {
		t.Fatalf("Authentication failed: %v", err)
	}

	if principal.Username != "alice" {
		t.Errorf("Expected username 'alice', got '%s'", principal.Username)
	}

	if len(principal.Roles) != 1 {
		t.Errorf("Expected 1 role, got %d", len(principal.Roles))
	}

	// Authenticate with wrong password
	badCredentials := Credentials{
		Type:     CredentialTypePassword,
		Username: "alice",
		Password: "wrongpassword",
	}

	_, err = auth.Authenticate(ctx, badCredentials)
	if err == nil {
		t.Error("Authentication with wrong password should fail")
	}
}

func TestCreateAPIKey(t *testing.T) {
	auth := NewDefaultAuthenticator()

	user := &User{
		ID:       "user-1",
		Username: "alice",
	}
	auth.RegisterUser(user)

	apiKey, err := auth.CreateAPIKey(user.ID, "Production Key", nil)
	if err != nil {
		t.Fatalf("Failed to create API key: %v", err)
	}

	if apiKey.Key == "" {
		t.Error("API key should be generated")
	}

	if apiKey.Name != "Production Key" {
		t.Errorf("Expected name 'Production Key', got '%s'", apiKey.Name)
	}

	if !apiKey.Active {
		t.Error("New API key should be active")
	}
}

func TestAuthenticateAPIKey(t *testing.T) {
	auth := NewDefaultAuthenticator()
	ctx := context.Background()

	// Register user
	user := &User{
		ID:           "user-1",
		Username:     "alice",
		Email:        "alice@example.com",
		PasswordHash: hashPassword("password123"),
		TenantID:     "tenant-1",
		Roles:        []string{"user"},
	}
	auth.RegisterUser(user)

	// Create API key
	apiKey, _ := auth.CreateAPIKey(user.ID, "Test Key", nil)

	// Authenticate with API key
	credentials := Credentials{
		Type:   CredentialTypeAPIKey,
		APIKey: apiKey.Key,
	}

	principal, err := auth.Authenticate(ctx, credentials)
	if err != nil {
		t.Fatalf("API key authentication failed: %v", err)
	}

	if principal.Username != "alice" {
		t.Errorf("Expected username 'alice', got '%s'", principal.Username)
	}

	// Check metadata
	if principal.Metadata["auth_method"] != "api_key" {
		t.Error("Auth method metadata not set")
	}
}

func TestValidateToken(t *testing.T) {
	auth := NewDefaultAuthenticator()
	ctx := context.Background()

	user := &User{
		ID:           "user-1",
		Username:     "alice",
		PasswordHash: hashPassword("password123"),
	}
	auth.RegisterUser(user)

	// Authenticate to get token
	credentials := Credentials{
		Type:     CredentialTypePassword,
		Username: "alice",
		Password: "password123",
	}

	principal, _ := auth.Authenticate(ctx, credentials)

	// Get token from session (simplified for test)
	auth.mu.RLock()
	var token string
	for t := range auth.sessions {
		token = t
		break
	}
	auth.mu.RUnlock()

	// Validate token
	validated, err := auth.ValidateToken(ctx, token)
	if err != nil {
		t.Fatalf("Token validation failed: %v", err)
	}

	if validated.ID != principal.ID {
		t.Error("Validated principal does not match")
	}
}

func TestRevokeToken(t *testing.T) {
	auth := NewDefaultAuthenticator()
	ctx := context.Background()

	user := &User{
		ID:           "user-1",
		Username:     "alice",
		PasswordHash: hashPassword("password123"),
	}
	auth.RegisterUser(user)

	// Authenticate
	credentials := Credentials{
		Type:     CredentialTypePassword,
		Username: "alice",
		Password: "password123",
	}
	auth.Authenticate(ctx, credentials)

	// Get token
	auth.mu.RLock()
	var token string
	for t := range auth.sessions {
		token = t
		break
	}
	auth.mu.RUnlock()

	// Revoke token
	err := auth.RevokeToken(ctx, token)
	if err != nil {
		t.Fatalf("Failed to revoke token: %v", err)
	}

	// Try to validate revoked token
	_, err = auth.ValidateToken(ctx, token)
	if err == nil {
		t.Error("Revoked token should not validate")
	}
}

func TestNewDefaultAuthorizer(t *testing.T) {
	authz := NewDefaultAuthorizer()

	if authz == nil {
		t.Fatal("Authorizer should not be nil")
	}
}

func TestGrantPermission(t *testing.T) {
	authz := NewDefaultAuthorizer()

	permission := Permission{
		Resource: "agents/*",
		Actions:  []string{"read", "execute"},
		Scope:    "tenant",
	}

	err := authz.GrantPermission("user", permission)
	if err != nil {
		t.Fatalf("Failed to grant permission: %v", err)
	}

	authz.mu.RLock()
	perms := authz.rolePermissions["user"]
	authz.mu.RUnlock()

	if len(perms) != 1 {
		t.Errorf("Expected 1 permission, got %d", len(perms))
	}
}

func TestAuthorize(t *testing.T) {
	authz := NewDefaultAuthorizer()

	// Grant permission
	authz.GrantPermission("developer", Permission{
		Resource: "agents/*",
		Actions:  []string{"read", "write", "execute"},
		Scope:    "global",
	})

	principal := &Principal{
		ID:       "user-1",
		Username: "alice",
		Roles:    []string{"developer"},
	}

	// Test allowed action
	allowed, err := authz.Authorize(principal, "agents/research-agent", "execute")
	if err != nil {
		t.Fatalf("Authorization check failed: %v", err)
	}

	if !allowed {
		t.Error("Action should be allowed")
	}

	// Test denied action
	allowed, err = authz.Authorize(principal, "agents/research-agent", "delete")
	if err != nil {
		t.Fatalf("Authorization check failed: %v", err)
	}

	if allowed {
		t.Error("Action should be denied")
	}
}

func TestWildcardPermissions(t *testing.T) {
	authz := NewDefaultAuthorizer()

	// Grant wildcard permission
	authz.GrantPermission("admin", Permission{
		Resource: "*",
		Actions:  []string{"*"},
		Scope:    "global",
	})

	principal := &Principal{
		ID:    "admin-1",
		Roles: []string{"admin"},
	}

	// Should allow any action on any resource
	allowed, _ := authz.Authorize(principal, "anything", "anything")

	if !allowed {
		t.Error("Wildcard permission should allow all actions")
	}
}

func TestRevokePermission(t *testing.T) {
	authz := NewDefaultAuthorizer()

	authz.GrantPermission("user", Permission{
		Resource: "agents/*",
		Actions:  []string{"read", "execute"},
		Scope:    "tenant",
	})

	err := authz.RevokePermission("user", "agents/*", "execute")
	if err != nil {
		t.Fatalf("Failed to revoke permission: %v", err)
	}

	principal := &Principal{
		ID:    "user-1",
		Roles: []string{"user"},
	}

	// Execute should now be denied
	allowed, _ := authz.Authorize(principal, "agents/test", "execute")
	if allowed {
		t.Error("Revoked action should be denied")
	}

	// Read should still be allowed
	allowed, _ = authz.Authorize(principal, "agents/test", "read")
	if !allowed {
		t.Error("Non-revoked action should still be allowed")
	}
}

func TestAuditLogger(t *testing.T) {
	logger := NewAuditLogger()

	logger.Log("user.login", "user-1", "auth", "login", "success", nil)
	logger.Log("agent.execute", "user-1", "agents/test", "execute", "success", map[string]interface{}{
		"duration": 1.5,
	})

	logs := logger.GetLogs(10)

	if len(logs) != 2 {
		t.Errorf("Expected 2 log entries, got %d", len(logs))
	}

	if logs[0].EventType != "user.login" {
		t.Error("First log entry not correct")
	}

	if logs[1].Metadata["duration"] != 1.5 {
		t.Error("Metadata not preserved")
	}
}

func TestExpiredAPIKey(t *testing.T) {
	auth := NewDefaultAuthenticator()
	ctx := context.Background()

	user := &User{
		ID:       "user-1",
		Username: "alice",
	}
	auth.RegisterUser(user)

	// Create expired API key
	expiresAt := time.Now().Add(-1 * time.Hour)
	apiKey, _ := auth.CreateAPIKey(user.ID, "Expired Key", &expiresAt)

	// Try to authenticate with expired key
	credentials := Credentials{
		Type:   CredentialTypeAPIKey,
		APIKey: apiKey.Key,
	}

	_, err := auth.Authenticate(ctx, credentials)
	if err == nil {
		t.Error("Authentication with expired API key should fail")
	}
}
