package database

import "database/sql"

// UserStore defines the interface for user-related database operations
type UserStore interface {
	GetUserByAccessKey(accessKey string) (*User, error)
	UpdateLastLogin(userID int) error
	GetUserPermissions(userID int) ([]UserPermission, error)
	CreateUser(user *User) error
	Close() error
}

// UserManagerStore defines the interface for UserManager (includes Exec and Select)
type UserManagerStore interface {
	GetUserByAccessKey(accessKey string) (*User, error)
	UpdateLastLogin(userID int) error
	CreateUser(user *User) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(dest interface{}, query string, args ...interface{}) error
}