package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/jmoiron/sqlx"
)

// DB represents the database connection
type DB struct {
	*sqlx.DB
}

// Ensure DB implements UserStore and UserManagerStore interfaces
var _ UserStore = (*DB)(nil)
var _ UserManagerStore = (*DB)(nil)

// Config holds database configuration
type Config struct {
	Driver          string
	ConnectionString string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// NewConnection creates a new database connection
func NewConnection(cfg Config) (*DB, error) {
	if cfg.Driver == "" {
		cfg.Driver = "postgres"
	}
	
	db, err := sqlx.Connect(cfg.Driver, cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Set connection pool settings
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	} else {
		db.SetMaxOpenConns(25)
	}

	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(5)
	}

	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	} else {
		db.SetConnMaxLifetime(5 * time.Minute)
	}

	return &DB{db}, nil
}

// GetUserByAccessKey retrieves a user by their access key
func (db *DB) GetUserByAccessKey(accessKey string) (*User, error) {
	var user User
	query := `SELECT id, access_key, secret_key, email, created_at, last_login, active 
	          FROM users WHERE access_key = $1 AND active = true`
	
	err := db.Get(&user, query, accessKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	
	return &user, nil
}

// UpdateLastLogin updates the last login timestamp for a user
func (db *DB) UpdateLastLogin(userID int) error {
	query := `UPDATE users SET last_login = $1 WHERE id = $2`
	_, err := db.Exec(query, time.Now(), userID)
	return err
}

// GetUserPermissions retrieves permissions for a user
func (db *DB) GetUserPermissions(userID int) ([]UserPermission, error) {
	var permissions []UserPermission
	query := `SELECT id, user_id, bucket_pattern, permissions 
	          FROM user_permissions WHERE user_id = $1`
	
	err := db.Select(&permissions, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user permissions: %w", err)
	}
	
	return permissions, nil
}

// CreateUser creates a new user
func (db *DB) CreateUser(user *User) error {
	query := `INSERT INTO users (access_key, secret_key, email, created_at, active, org_id, role)
	          VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`

	role := user.Role
	if role == "" {
		role = "member"
	}

	err := db.Get(&user.ID, query, user.AccessKey, user.SecretKey, user.Email, time.Now(), true, user.OrgID, role)
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}

// CreateOrganization creates a new organization
func (db *DB) CreateOrganization(org *Organization) error {
	query := `INSERT INTO organizations (name, slug, active, settings)
	          VALUES ($1, $2, $3, $4) RETURNING id, created_at, updated_at`
	return db.QueryRow(query, org.Name, org.Slug, true, org.Settings).Scan(&org.ID, &org.CreatedAt, &org.UpdatedAt)
}

// GetOrgBySlug retrieves an organization by its slug
func (db *DB) GetOrgBySlug(slug string) (*Organization, error) {
	var org Organization
	query := `SELECT id, name, slug, created_at, updated_at, active, settings
	          FROM organizations WHERE slug = $1 AND active = true`
	err := db.Get(&org, query, slug)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get organization: %w", err)
	}
	return &org, nil
}

// GetOrgByID retrieves an organization by its ID
func (db *DB) GetOrgByID(id string) (*Organization, error) {
	var org Organization
	query := `SELECT id, name, slug, created_at, updated_at, active, settings
	          FROM organizations WHERE id = $1 AND active = true`
	err := db.Get(&org, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get organization: %w", err)
	}
	return &org, nil
}

// ListOrganizations retrieves all active organizations
func (db *DB) ListOrganizations() ([]Organization, error) {
	var orgs []Organization
	query := `SELECT id, name, slug, created_at, updated_at, active, settings
	          FROM organizations WHERE active = true ORDER BY created_at DESC`
	err := db.Select(&orgs, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list organizations: %w", err)
	}
	return orgs, nil
}

// GetUsersByOrgID retrieves all users belonging to an organization
func (db *DB) GetUsersByOrgID(orgID string) ([]User, error) {
	var users []User
	query := `SELECT id, access_key, secret_key, email, created_at, last_login, active, org_id, role
	          FROM users WHERE org_id = $1 ORDER BY created_at DESC`
	err := db.Select(&users, query, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to get users by org: %w", err)
	}
	return users, nil
}

// DeleteUserByID deletes a user by their ID
func (db *DB) DeleteUserByID(userID int) error {
	query := `DELETE FROM users WHERE id = $1`
	_, err := db.Exec(query, userID)
	return err
}

// CreateBucketMapping creates a virtual bucket mapping for an org
func (db *DB) CreateBucketMapping(mapping *OrgBucketMapping) error {
	query := `INSERT INTO org_bucket_mappings (org_id, virtual_bucket, physical_bucket, prefix)
	          VALUES ($1, $2, $3, $4) RETURNING id, created_at`
	return db.QueryRow(query, mapping.OrgID, mapping.VirtualBucket, mapping.PhysicalBucket, mapping.Prefix).
		Scan(&mapping.ID, &mapping.CreatedAt)
}

// GetBucketMappingsByOrgID retrieves all bucket mappings for an org
func (db *DB) GetBucketMappingsByOrgID(orgID string) ([]OrgBucketMapping, error) {
	var mappings []OrgBucketMapping
	query := `SELECT id, org_id, virtual_bucket, physical_bucket, prefix, created_at
	          FROM org_bucket_mappings WHERE org_id = $1 ORDER BY virtual_bucket`
	err := db.Select(&mappings, query, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket mappings: %w", err)
	}
	return mappings, nil
}

// GetBucketMapping retrieves a specific bucket mapping
func (db *DB) GetBucketMapping(orgID, virtualBucket string) (*OrgBucketMapping, error) {
	var mapping OrgBucketMapping
	query := `SELECT id, org_id, virtual_bucket, physical_bucket, prefix, created_at
	          FROM org_bucket_mappings WHERE org_id = $1 AND virtual_bucket = $2`
	err := db.Get(&mapping, query, orgID, virtualBucket)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get bucket mapping: %w", err)
	}
	return &mapping, nil
}

// DeleteBucketMapping deletes a virtual bucket mapping
func (db *DB) DeleteBucketMapping(orgID, virtualBucket string) error {
	query := `DELETE FROM org_bucket_mappings WHERE org_id = $1 AND virtual_bucket = $2`
	_, err := db.Exec(query, orgID, virtualBucket)
	return err
}