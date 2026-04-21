package security

import (
	"errors"
	"path/filepath"
	"strings"
	"unicode"
)

var (
	ErrPathTraversal   = errors.New("path contains traversal sequences")
	ErrInvalidPath     = errors.New("path contains invalid characters")
	ErrAbsolutePath    = errors.New("absolute paths not allowed")
	ErrEmptyPath       = errors.New("path cannot be empty")
	ErrPathOutsideBase = errors.New("path resolves outside base directory")
)

// ValidatePathSecure performs comprehensive path validation to prevent traversal attacks
func ValidatePathSecure(path string) error {
	if path == "" {
		return ErrEmptyPath
	}

	// Check for null bytes first
	if strings.ContainsRune(path, 0) {
		return ErrInvalidPath
	}

	// Check for traversal sequences before cleaning
	if strings.Contains(path, "..") {
		return ErrPathTraversal
	}

	// Reject absolute paths
	if filepath.IsAbs(path) {
		return ErrAbsolutePath
	}

	// Clean the path using OS-aware cleaning
	cleaned := filepath.Clean(path)

	// After cleaning, double-check for traversal sequences
	if strings.Contains(cleaned, "..") {
		return ErrPathTraversal
	}

	// Check for dangerous characters
	for _, char := range cleaned {
		if char < 32 && char != 9 && char != 10 && char != 13 {
			return ErrInvalidPath
		}
	}

	return nil
}

// SecurePath validates and returns a secure path within a base directory
func SecurePath(basePath, userPath string) (string, error) {
	if err := ValidatePathSecure(userPath); err != nil {
		return "", err
	}

	// Join and clean the full path
	fullPath := filepath.Join(basePath, userPath)
	cleanFull := filepath.Clean(fullPath)
	cleanBase := filepath.Clean(basePath)

	// Ensure the resolved path is within the base directory
	if !strings.HasPrefix(cleanFull, cleanBase+string(filepath.Separator)) && cleanFull != cleanBase {
		return "", ErrPathOutsideBase
	}

	return cleanFull, nil
}

// ValidateBucketName validates S3 bucket names with security checks
func ValidateBucketName(bucket string) error {
	if bucket == "" {
		return ErrEmptyPath
	}

	// Basic path validation
	if err := ValidatePathSecure(bucket); err != nil {
		return err
	}

	// Bucket-specific checks
	if strings.Contains(bucket, "/") || strings.Contains(bucket, "\\") {
		return ErrInvalidPath
	}

	if bucket == "." || bucket == ".." {
		return ErrInvalidPath
	}

	return nil
}

// ValidateObjectKey validates S3 object keys with security checks
func ValidateObjectKey(key string) error {
	if key == "" {
		return ErrEmptyPath
	}

	// Use path validation as base
	if err := ValidatePathSecure(key); err != nil {
		return err
	}

	// Disallow backslashes to prevent alternate path traversal forms
	if strings.ContainsRune(key, '\\') {
		return ErrInvalidPath
	}

	// Block absolute filesystem paths (which would also be caught by
	// ValidatePathSecure's IsAbs check, but be explicit). We intentionally
	// do NOT match these as substrings — arbitrary S3 keys like
	// `infrastructure/vault/overlays/dev/serviceaccount.yaml` contain `/dev/`
	// as a normal path segment and have nothing to do with the host's
	// `/dev` filesystem. Matching substrings here would reject legitimate
	// Flux/Kustomize layouts that use `dev`, `etc`, `var`, `proc`, `sys`
	// as directory names.
	lowerKey := strings.ToLower(key)
	dangerousPrefixes := []string{
		"/etc/", "/proc/", "/sys/", "/dev/", "/var/",
	}
	for _, p := range dangerousPrefixes {
		if strings.HasPrefix(lowerKey, p) {
			return ErrInvalidPath
		}
	}

	// Windows-style absolute paths and traversal tokens anywhere in the
	// key are always suspect — reject regardless of position.
	windowsDangerous := []string{
		"\\windows\\", "\\system32\\", "\\program files\\",
		"../", ".\\", "..\\",
	}
	for _, d := range windowsDangerous {
		if strings.Contains(lowerKey, d) {
			return ErrInvalidPath
		}
	}

	return nil
}

// SanitizePathAllowlist validates path using character allowlist approach
func SanitizePathAllowlist(path string) (string, error) {
	if path == "" {
		return "", ErrEmptyPath
	}

	// Check for null bytes before any processing
	if strings.ContainsRune(path, 0) {
		return "", ErrInvalidPath
	}

	// Check for double slashes before cleaning (security requirement)
	if strings.Contains(path, "//") {
		return "", ErrInvalidPath
	}

	// Check for absolute paths
	if filepath.IsAbs(path) {
		return "", ErrAbsolutePath
	}

	// Check for traversal sequences before cleaning
	if strings.Contains(path, "..") {
		return "", ErrPathTraversal
	}

	// Clean the path
	cleaned := filepath.Clean(path)

	// Double-check for traversal after cleaning
	if strings.Contains(cleaned, "..") {
		return "", ErrPathTraversal
	}

	// Character allowlist validation
	for _, char := range cleaned {
		if !isAllowedPathChar(char) {
			return "", ErrInvalidPath
		}
	}

	return cleaned, nil
}

// isAllowedPathChar returns true if the character is allowed in paths
func isAllowedPathChar(r rune) bool {
	return unicode.IsLetter(r) ||
		unicode.IsDigit(r) ||
		r == '/' || r == '-' || r == '_' || r == '.' || r == ' '
}
