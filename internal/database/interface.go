package database

import "database/sql"

// UserStore defines the interface for user-related database operations
type UserStore interface {
	GetUserByAccessKey(accessKey string) (*User, error)
	UpdateLastLogin(userID int) error
	GetUserPermissions(userID int) ([]UserPermission, error)
	CreateUser(user *User) error
	Close() error

	// Organization operations
	CreateOrganization(org *Organization) error
	GetOrgBySlug(slug string) (*Organization, error)
	GetOrgByID(id string) (*Organization, error)
	ListOrganizations() ([]Organization, error)
	GetUsersByOrgID(orgID string) ([]User, error)
	DeleteUserByID(userID int) error

	// Bucket mapping operations
	CreateBucketMapping(mapping *OrgBucketMapping) error
	GetBucketMappingsByOrgID(orgID string) ([]OrgBucketMapping, error)
	GetBucketMapping(orgID, virtualBucket string) (*OrgBucketMapping, error)
	DeleteBucketMapping(orgID, virtualBucket string) error
}

// UserManagerStore defines the interface for UserManager (includes Exec and Select)
type UserManagerStore interface {
	GetUserByAccessKey(accessKey string) (*User, error)
	UpdateLastLogin(userID int) error
	CreateUser(user *User) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(dest interface{}, query string, args ...interface{}) error
}