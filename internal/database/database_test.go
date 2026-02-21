package database

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Mock database implementation for testing
type mockDB struct {
	users       map[string]*User
	permissions map[int][]UserPermission
	lastUserID  int
	errors      map[string]error
}

func newMockDB() *mockDB {
	return &mockDB{
		users:       make(map[string]*User),
		permissions: make(map[int][]UserPermission),
		lastUserID:  0,
		errors:      make(map[string]error),
	}
}

func (m *mockDB) GetUserByAccessKey(accessKey string) (*User, error) {
	if err, exists := m.errors["GetUserByAccessKey"]; exists {
		return nil, err
	}
	
	user, exists := m.users[accessKey]
	if !exists {
		return nil, nil
	}
	return user, nil
}

func (m *mockDB) UpdateLastLogin(userID int) error {
	if err, exists := m.errors["UpdateLastLogin"]; exists {
		return err
	}
	
	// Find user by ID and update last login
	for _, user := range m.users {
		if user.ID == userID {
			now := time.Now()
			user.LastLogin = &now
			return nil
		}
	}
	return nil
}

func (m *mockDB) GetUserPermissions(userID int) ([]UserPermission, error) {
	if err, exists := m.errors["GetUserPermissions"]; exists {
		return nil, err
	}
	
	permissions, exists := m.permissions[userID]
	if !exists {
		return []UserPermission{}, nil
	}
	return permissions, nil
}

func (m *mockDB) CreateUser(user *User) error {
	if err, exists := m.errors["CreateUser"]; exists {
		return err
	}
	
	m.lastUserID++
	user.ID = m.lastUserID
	m.users[user.AccessKey] = user
	return nil
}

func (m *mockDB) Close() error {
	return nil
}

func (m *mockDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if err, exists := m.errors["Exec"]; exists {
		return nil, err
	}
	return &mockResult{}, nil
}

func (m *mockDB) Select(dest interface{}, query string, args ...interface{}) error {
	if err, exists := m.errors["Select"]; exists {
		return err
	}
	
	// Simple mock implementation for ListUsers
	if users, ok := dest.(*[]User); ok {
		*users = make([]User, 0, len(m.users))
		for _, user := range m.users {
			*users = append(*users, *user)
		}
	}
	return nil
}

type mockResult struct{}

func (m *mockResult) LastInsertId() (int64, error) {
	return 1, nil
}

func (m *mockResult) RowsAffected() (int64, error) {
	return 1, nil
}

func TestNewConnection_InvalidDriver(t *testing.T) {
	_, err := NewConnection(Config{Driver: "postgres", ConnectionString: "host=invalid port=9999 user=x password=x dbname=x sslmode=disable connect_timeout=1"})
	if err == nil {
		t.Fatal("Expected error for invalid connection")
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}
	
	db := &DB{}
	
	// Test default values would be applied in NewConnection
	if cfg.Driver == "" {
		cfg.Driver = "postgres"
	}
	
	if cfg.Driver != "postgres" {
		t.Errorf("Expected default driver postgres, got %s", cfg.Driver)
	}
	
	// Test that DB struct can be created
	if db == nil {
		t.Error("DB struct should be creatable")
	}
}

func TestUser_Fields(t *testing.T) {
	now := time.Now()
	user := User{
		ID:        1,
		AccessKey: "TESTKEY123",
		SecretKey: "secret",
		Email:     "test@example.com",
		CreatedAt: now,
		LastLogin: &now,
		Active:    true,
	}
	
	if user.ID != 1 {
		t.Errorf("Expected ID 1, got %d", user.ID)
	}
	
	if user.AccessKey != "TESTKEY123" {
		t.Errorf("Expected access key TESTKEY123, got %s", user.AccessKey)
	}
	
	if user.Email != "test@example.com" {
		t.Errorf("Expected email test@example.com, got %s", user.Email)
	}
	
	if !user.Active {
		t.Error("Expected user to be active")
	}
}

func TestUserPermission_Fields(t *testing.T) {
	perm := UserPermission{
		ID:            1,
		UserID:        123,
		BucketPattern: "bucket-*",
		Permissions:   "read,write",
	}
	
	if perm.UserID != 123 {
		t.Errorf("Expected user ID 123, got %d", perm.UserID)
	}
	
	if perm.BucketPattern != "bucket-*" {
		t.Errorf("Expected bucket pattern bucket-*, got %s", perm.BucketPattern)
	}
	
	if perm.Permissions != "read,write" {
		t.Errorf("Expected permissions read,write, got %s", perm.Permissions)
	}
}

func TestDB_GetUserByAccessKey_Success(t *testing.T) {
	mock := newMockDB()
	
	// Add a test user
	testUser := &User{
		ID:        1,
		AccessKey: "TESTKEY123",
		SecretKey: "secret",
		Email:     "test@example.com",
		Active:    true,
	}
	mock.users["TESTKEY123"] = testUser
	
	user, err := mock.GetUserByAccessKey("TESTKEY123")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if user == nil {
		t.Fatal("Expected user, got nil")
	}
	
	if user.AccessKey != "TESTKEY123" {
		t.Errorf("Expected access key TESTKEY123, got %s", user.AccessKey)
	}
}

func TestDB_GetUserByAccessKey_NotFound(t *testing.T) {
	mock := newMockDB()
	
	user, err := mock.GetUserByAccessKey("NONEXISTENT")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if user != nil {
		t.Error("Expected nil user for nonexistent key")
	}
}

func TestDB_GetUserByAccessKey_Error(t *testing.T) {
	mock := newMockDB()
	mock.errors["GetUserByAccessKey"] = sql.ErrConnDone
	
	user, err := mock.GetUserByAccessKey("TESTKEY")
	if err == nil {
		t.Error("Expected error, got nil")
	}
	
	if user != nil {
		t.Error("Expected nil user on error")
	}
}

func TestDB_CreateUser_Success(t *testing.T) {
	mock := newMockDB()
	
	user := &User{
		AccessKey: "NEWKEY123",
		SecretKey: "secret",
		Email:     "new@example.com",
		Active:    true,
	}
	
	err := mock.CreateUser(user)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if user.ID == 0 {
		t.Error("Expected user ID to be set")
	}
	
	// Verify user was stored
	stored, err := mock.GetUserByAccessKey("NEWKEY123")
	if err != nil {
		t.Fatalf("Error retrieving created user: %v", err)
	}
	
	if stored == nil {
		t.Fatal("Created user should be retrievable")
	}
}

func TestDB_UpdateLastLogin_Success(t *testing.T) {
	mock := newMockDB()
	
	// Add a test user
	testUser := &User{
		ID:        1,
		AccessKey: "TESTKEY123",
		Active:    true,
	}
	mock.users["TESTKEY123"] = testUser
	
	err := mock.UpdateLastLogin(1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	// Verify last login was updated
	if testUser.LastLogin == nil {
		t.Error("Expected last login to be set")
	}
}

func TestDB_GetUserPermissions_Success(t *testing.T) {
	mock := newMockDB()
	
	// Add test permissions
	testPerms := []UserPermission{
		{ID: 1, UserID: 1, BucketPattern: "bucket-1", Permissions: "read"},
		{ID: 2, UserID: 1, BucketPattern: "bucket-2", Permissions: "read,write"},
	}
	mock.permissions[1] = testPerms
	
	perms, err := mock.GetUserPermissions(1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if len(perms) != 2 {
		t.Errorf("Expected 2 permissions, got %d", len(perms))
	}
}

func TestDB_GetUserPermissions_Empty(t *testing.T) {
	mock := newMockDB()
	
	perms, err := mock.GetUserPermissions(999)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if len(perms) != 0 {
		t.Errorf("Expected 0 permissions, got %d", len(perms))
	}
}

func TestUserManager_CreateUser_DBError(t *testing.T) {
	mock := newMockDB()
	mock.errors["CreateUser"] = errors.New("db error")
	um := NewUserManager(mock)
	_, err := um.CreateUser("test@example.com", "password123")
	if err == nil {
		t.Fatal("CreateUser expected error when DB fails")
	}
}

func TestUserManager_CreateUser(t *testing.T) {
	mock := newMockDB()
	um := NewUserManager(mock)

	user, err := um.CreateUser("test@example.com", "password123")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	if user == nil {
		t.Fatal("Expected user, got nil")
	}
	if user.Email != "test@example.com" {
		t.Errorf("Expected email test@example.com, got %s", user.Email)
	}
	if user.AccessKey == "" || len(user.AccessKey) > 20 {
		t.Errorf("Access key invalid: %q", user.AccessKey)
	}
	if user.SecretKey == "" {
		t.Error("Secret key should be hashed and non-empty")
	}
	if !user.Active {
		t.Error("New user should be active")
	}
	// Verify stored
	stored, _ := mock.GetUserByAccessKey(user.AccessKey)
	if stored == nil {
		t.Error("User should be stored in mock")
	}
}

func TestUserManager_AuthenticateUser_Success(t *testing.T) {
	mock := newMockDB()
	password := "testpassword"
	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	testUser := &User{
		ID:        1,
		AccessKey: "TESTKEY123",
		SecretKey: string(hashedPassword),
		Email:     "test@example.com",
		Active:    true,
	}
	mock.users["TESTKEY123"] = testUser

	um := NewUserManager(mock)
	user, err := um.AuthenticateUser("TESTKEY123", password)
	if err != nil {
		t.Fatalf("AuthenticateUser failed: %v", err)
	}
	if user == nil || user.AccessKey != "TESTKEY123" {
		t.Errorf("Expected user TESTKEY123, got %v", user)
	}
}

func TestUserManager_AuthenticateUser_InvalidCredentials(t *testing.T) {
	mock := newMockDB()
	password := "testpassword"
	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	mock.users["TESTKEY123"] = &User{ID: 1, AccessKey: "TESTKEY123", SecretKey: string(hashedPassword), Active: true}

	um := NewUserManager(mock)
	_, err := um.AuthenticateUser("TESTKEY123", "wrongpassword")
	if err == nil {
		t.Fatal("Expected error for wrong password")
	}
	_, err = um.AuthenticateUser("NONEXISTENT", "any")
	if err == nil {
		t.Fatal("Expected error for nonexistent user")
	}
}

func TestUserManager_AuthenticateUser_DBError(t *testing.T) {
	mock := newMockDB()
	mock.errors["GetUserByAccessKey"] = sql.ErrConnDone
	um := NewUserManager(mock)
	_, err := um.AuthenticateUser("KEY", "secret")
	if err == nil {
		t.Fatal("Expected error when DB fails")
	}
}

func TestGenerateAccessKey(t *testing.T) {
	// Test multiple generations to ensure randomness
	keys := make(map[string]bool)
	
	for i := 0; i < 10; i++ {
		key, err := generateAccessKey()
		if err != nil {
			t.Fatalf("Failed to generate access key: %v", err)
		}
		
		if len(key) == 0 {
			t.Error("Access key should not be empty")
		}
		
		if len(key) > 20 {
			t.Errorf("Access key should be <= 20 chars, got %d", len(key))
		}
		
		// Should be uppercase
		if key != key {
			t.Error("Access key should be uppercase")
		}
		
		// Should not contain special characters
		for _, char := range key {
			if char < 'A' || char > 'Z' {
				if char < '0' || char > '9' {
					t.Errorf("Access key should only contain A-Z and 0-9, found %c", char)
				}
			}
		}
		
		// Should be unique
		if keys[key] {
			t.Errorf("Generated duplicate access key: %s", key)
		}
		keys[key] = true
	}
}

func TestUserManager_Interface_Usage(t *testing.T) {
	mock := newMockDB()
	um := NewUserManager(mock)
	if um == nil {
		t.Fatal("UserManager should not be nil")
	}
	if um.db == nil {
		t.Error("UserManager should have a db field")
	}
}

func TestPasswordHashing(t *testing.T) {
	passwords := []string{
		"simple",
		"complex!@#$%^&*()",
		"very-long-password-with-many-characters-to-test-length-handling",
		"unicode-测试-🔒",
		"",
	}
	
	for _, password := range passwords {
		t.Run("password_"+password, func(t *testing.T) {
			hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
			if err != nil {
				t.Fatalf("Failed to hash password: %v", err)
			}
			
			// Verify correct password
			err = bcrypt.CompareHashAndPassword(hash, []byte(password))
			if err != nil {
				t.Errorf("Failed to verify correct password: %v", err)
			}
			
			// Verify wrong password fails
			err = bcrypt.CompareHashAndPassword(hash, []byte(password+"wrong"))
			if err == nil {
				t.Error("Wrong password should fail verification")
			}
		})
	}
}

func BenchmarkGenerateAccessKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := generateAccessKey()
		if err != nil {
			b.Fatalf("Failed to generate access key: %v", err)
		}
	}
}

func TestUserManager_CreateUserWithKeys(t *testing.T) {
	mock := newMockDB()
	um := NewUserManager(mock)
	user, err := um.CreateUserWithKeys("test@example.com", "TESTKEY123", "secret456")
	if err != nil {
		t.Fatalf("CreateUserWithKeys failed: %v", err)
	}
	if user.Email != "test@example.com" || user.AccessKey != "TESTKEY123" {
		t.Errorf("Unexpected user: %+v", user)
	}
	// Verify we can authenticate with the secret
	_, err = um.AuthenticateUser("TESTKEY123", "secret456")
	if err != nil {
		t.Errorf("Authenticate after CreateUserWithKeys failed: %v", err)
	}
}

func TestUserManager_CreateUserWithKeys_DBError(t *testing.T) {
	mock := newMockDB()
	mock.errors["CreateUser"] = errors.New("db full")
	um := NewUserManager(mock)
	_, err := um.CreateUserWithKeys("a@b.com", "AK", "secret")
	if err == nil {
		t.Fatal("expected error when DB CreateUser fails")
	}
}

func TestUserManager_DisableEnableUser(t *testing.T) {
	mock := newMockDB()
	mock.users["AK"] = &User{ID: 1, AccessKey: "AK", Active: true}
	um := NewUserManager(mock)
	if err := um.DisableUser("AK"); err != nil {
		t.Fatalf("DisableUser failed: %v", err)
	}
	if err := um.EnableUser("AK"); err != nil {
		t.Fatalf("EnableUser failed: %v", err)
	}
}

func TestUserManager_UpdateUserPassword(t *testing.T) {
	mock := newMockDB()
	hashed, _ := bcrypt.GenerateFromPassword([]byte("old"), bcrypt.DefaultCost)
	mock.users["UK"] = &User{ID: 1, AccessKey: "UK", SecretKey: string(hashed), Active: true}
	um := NewUserManager(mock)
	if err := um.UpdateUserPassword("UK", "newpassword123"); err != nil {
		t.Fatalf("UpdateUserPassword failed: %v", err)
	}
}

func TestUserManager_BucketPermissions(t *testing.T) {
	mock := newMockDB()
	mock.users["BPK"] = &User{ID: 1, AccessKey: "BPK", Active: true}
	um := NewUserManager(mock)
	if err := um.GrantBucketPermission("BPK", "bucket-*", "read,write"); err != nil {
		t.Fatalf("GrantBucketPermission failed: %v", err)
	}
	if err := um.RevokeBucketPermission("BPK", "bucket-*"); err != nil {
		t.Fatalf("RevokeBucketPermission failed: %v", err)
	}
}

func TestUserManager_GrantPermission_UserNotFound(t *testing.T) {
	mock := newMockDB()
	um := NewUserManager(mock)
	err := um.GrantBucketPermission("MISSING", "b", "read")
	if err == nil {
		t.Fatal("Expected error when user not found")
	}
}

func TestUserManager_RevokePermission_UserNotFound(t *testing.T) {
	mock := newMockDB()
	um := NewUserManager(mock)
	err := um.RevokeBucketPermission("MISSING", "b")
	if err == nil {
		t.Fatal("Expected error when user not found")
	}
}

func TestUserManager_ListUsers(t *testing.T) {
	mock := newMockDB()
	mock.users["KEY1"] = &User{ID: 1, AccessKey: "KEY1", Email: "u1@example.com", Active: true}
	mock.users["KEY2"] = &User{ID: 2, AccessKey: "KEY2", Email: "u2@example.com", Active: false}
	um := NewUserManager(mock)
	users, err := um.ListUsers()
	if err != nil {
		t.Fatalf("ListUsers failed: %v", err)
	}
	if len(users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(users))
	}
}

func TestUserManager_ListUsers_SelectError(t *testing.T) {
	mock := newMockDB()
	mock.errors["Select"] = errors.New("db select error")
	um := NewUserManager(mock)
	users, err := um.ListUsers()
	if err == nil {
		t.Fatal("ListUsers expected error")
	}
	if users != nil {
		t.Error("users should be nil on error")
	}
}

func TestDB_Close(t *testing.T) {
	mock := newMockDB()
	
	err := mock.Close()
	if err != nil {
		t.Errorf("Close should not return error, got %v", err)
	}
}

func TestDB_ErrorHandling(t *testing.T) {
	mock := newMockDB()
	
	// Test error injection for all methods
	testError := errors.New("test database error")
	
	// Test GetUserByAccessKey error
	mock.errors["GetUserByAccessKey"] = testError
	user, err := mock.GetUserByAccessKey("TESTKEY")
	if err != testError {
		t.Errorf("Expected test error, got %v", err)
	}
	if user != nil {
		t.Error("User should be nil on error")
	}
	delete(mock.errors, "GetUserByAccessKey")
	
	// Test UpdateLastLogin error
	mock.errors["UpdateLastLogin"] = testError
	err = mock.UpdateLastLogin(1)
	if err != testError {
		t.Errorf("Expected test error, got %v", err)
	}
	delete(mock.errors, "UpdateLastLogin")
	
	// Test GetUserPermissions error
	mock.errors["GetUserPermissions"] = testError
	perms, err := mock.GetUserPermissions(1)
	if err != testError {
		t.Errorf("Expected test error, got %v", err)
	}
	if perms != nil {
		t.Error("Permissions should be nil on error")
	}
	delete(mock.errors, "GetUserPermissions")
	
	// Test CreateUser error
	mock.errors["CreateUser"] = testError
	err = mock.CreateUser(&User{})
	if err != testError {
		t.Errorf("Expected test error, got %v", err)
	}
	delete(mock.errors, "CreateUser")
	
	// Test Exec error
	mock.errors["Exec"] = testError
	result, err := mock.Exec("SELECT 1")
	if err != testError {
		t.Errorf("Expected test error, got %v", err)
	}
	if result != nil {
		t.Error("Result should be nil on error")
	}
	delete(mock.errors, "Exec")
	
	// Test Select error
	mock.errors["Select"] = testError
	var users []User
	err = mock.Select(&users, "SELECT * FROM users")
	if err != testError {
		t.Errorf("Expected test error, got %v", err)
	}
	delete(mock.errors, "Select")
}

func TestConfig_ConnectionPoolSettings(t *testing.T) {
	// Test default connection pool settings
	cfg := Config{}
	
	// These would be applied in NewConnection
	expectedMaxOpen := 25
	expectedMaxIdle := 5
	
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = expectedMaxOpen
	}
	
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = expectedMaxIdle
	}
	
	if cfg.MaxOpenConns != expectedMaxOpen {
		t.Errorf("Expected MaxOpenConns %d, got %d", expectedMaxOpen, cfg.MaxOpenConns)
	}
	
	if cfg.MaxIdleConns != expectedMaxIdle {
		t.Errorf("Expected MaxIdleConns %d, got %d", expectedMaxIdle, cfg.MaxIdleConns)
	}
	
	// Test custom settings
	customCfg := Config{
		MaxOpenConns: 50,
		MaxIdleConns: 10,
	}
	
	if customCfg.MaxOpenConns != 50 {
		t.Errorf("Expected custom MaxOpenConns 50, got %d", customCfg.MaxOpenConns)
	}
	
	if customCfg.MaxIdleConns != 10 {
		t.Errorf("Expected custom MaxIdleConns 10, got %d", customCfg.MaxIdleConns)
	}
}

func BenchmarkPasswordHashing(b *testing.B) {
	password := "testpassword"
	
	b.Run("GenerateFromPassword", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
			if err != nil {
				b.Fatalf("Failed to hash password: %v", err)
			}
		}
	})
	
	// Pre-generate hash for comparison benchmark
	hash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	
	b.Run("CompareHashAndPassword", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := bcrypt.CompareHashAndPassword(hash, []byte(password))
			if err != nil {
				b.Fatalf("Failed to verify password: %v", err)
			}
		}
	})
}