package security

import (
	"strings"
	"testing"
)

func TestValidatePathSecure(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"valid path", "bucket/file.txt", false},
		{"empty path", "", true},
		{"null byte", "bucket/file\x00.txt", true},
		{"parent directory", "bucket/../file.txt", true},
		{"absolute path", "/etc/passwd", true},
		{"windows traversal", "bucket\\..\\file.txt", true},
		{"clean traversal", "bucket/file.txt", false},
		{"control char", "bucket/file\x01.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePathSecure(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePathSecure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSecurePath(t *testing.T) {
	baseDir := "/safe/base"
	
	tests := []struct {
		name     string
		userPath string
		wantErr  bool
		contains string
	}{
		{"safe path", "bucket/file.txt", false, "/safe/base/bucket/file.txt"},
		{"traversal attempt", "../../../etc/passwd", true, ""},
		{"null byte", "bucket\x00/file.txt", true, ""},
		{"complex traversal", "bucket/../../../etc/passwd", true, ""},
		{"base only", ".", false, "/safe/base"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SecurePath(baseDir, tt.userPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("SecurePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !strings.Contains(result, tt.contains) {
				t.Errorf("SecurePath() result = %v, want to contain %v", result, tt.contains)
			}
		})
	}
}

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		wantErr bool
	}{
		{"valid bucket", "mybucket", false},
		{"empty bucket", "", true},
		{"traversal bucket", "../bucket", true},
		{"slash bucket", "bucket/name", true},
		{"dot bucket", ".", true},
		{"dotdot bucket", "..", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.bucket)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBucketName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateObjectKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid key", "folder/file.txt", false},
		{"empty key", "", true},
		{"traversal key", "../file.txt", true},
		{"system path", "/etc/passwd", true},
		{"windows system", "\\windows\\system32\\file", true},
		{"complex traversal", "folder/../../../etc/passwd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateObjectKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateObjectKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSanitizePathAllowlist(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantErr  bool
		wantPath string
	}{
		{"valid", "bucket/file.txt", false, "bucket/file.txt"},
		{"empty", "", true, ""},
		{"null byte", "bucket\x00/file", true, ""},
		{"double slash", "bucket//file", true, ""},
		{"absolute", "/etc/passwd", true, ""},
		{"traversal", "bucket/../file", true, ""},
		{"cleaned path", "bucket/./file.txt", false, "bucket/file.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizePathAllowlist(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizePathAllowlist() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.wantPath {
				t.Errorf("SanitizePathAllowlist() = %q, want %q", got, tt.wantPath)
			}
		})
	}
}