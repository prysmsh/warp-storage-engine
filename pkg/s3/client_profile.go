package s3

import (
	"context"
	"net/http"
	"strings"
)

// ClientProfile captures capabilities inferred from the incoming request.
type ClientProfile struct {
	UserAgent string

	// Base capability flags
	JavaSDK bool
	AWSCLI  bool
	Browser bool
	MinIO   bool
	SDKv2   bool

	// Additional traits used for logging/optimisations
	Trino  bool
	Presto bool
	Hive   bool
	Hadoop bool
	Spark  bool
	S3A    bool
}

type clientProfileContextKey struct{}

// DetectClientProfile analyzes the request and returns the derived client profile.
func DetectClientProfile(r *http.Request) ClientProfile {
	profile := detectClientProfileFromUserAgent(r.Header.Get("User-Agent"))

	// Detect AWS SDK v2 style requests via headers or query params.
	if r.Header.Get("x-amz-sdk-request") != "" ||
		r.Header.Get("x-amz-checksum-algorithm") != "" ||
		r.URL.Query().Get("x-id") != "" {
		profile.SDKv2 = true
	}

	return profile
}

// WithClientProfile enriches the request context with the detected profile.
func WithClientProfile(r *http.Request) (*http.Request, ClientProfile) {
	profile := DetectClientProfile(r)
	ctx := context.WithValue(r.Context(), clientProfileContextKey{}, profile)
	return r.WithContext(ctx), profile
}

// GetClientProfile returns the client profile attached to the request (or detects it on the fly).
func GetClientProfile(r *http.Request) ClientProfile {
	if value := r.Context().Value(clientProfileContextKey{}); value != nil {
		if profile, ok := value.(ClientProfile); ok {
			return profile
		}
	}
	return DetectClientProfile(r)
}

// Labels returns a slice of human-readable labels describing the client.
func (cp ClientProfile) Labels() []string {
	labels := make([]string, 0, 6)

	if cp.JavaSDK {
		labels = append(labels, "java-sdk")
	}
	if cp.AWSCLI {
		labels = append(labels, "aws-cli")
	}
	if cp.Browser {
		labels = append(labels, "browser")
	}
	if cp.MinIO {
		labels = append(labels, "minio")
	}
	if cp.SDKv2 {
		labels = append(labels, "aws-sdk-v2")
	}
	if cp.Trino {
		labels = append(labels, "trino")
	}
	if cp.Presto {
		labels = append(labels, "presto")
	}
	if cp.Hive {
		labels = append(labels, "hive")
	}
	if cp.Hadoop {
		labels = append(labels, "hadoop")
	}
	if cp.Spark {
		labels = append(labels, "spark")
	}
	if cp.S3A {
		labels = append(labels, "s3a")
	}

	if len(labels) == 0 {
		return []string{"unknown"}
	}
	return labels
}

func detectClientProfileFromUserAgent(userAgent string) ClientProfile {
	lowerUA := strings.ToLower(userAgent)

	profile := ClientProfile{
		UserAgent: userAgent,
	}

	profile.Trino = strings.Contains(lowerUA, "trino")
	profile.Presto = strings.Contains(lowerUA, "presto")
	profile.Hive = strings.Contains(lowerUA, "hive")
	profile.Hadoop = strings.Contains(lowerUA, "hadoop")
	profile.Spark = strings.Contains(lowerUA, "spark")
	profile.S3A = strings.Contains(lowerUA, "s3a")

	profile.JavaSDK = profile.Trino ||
		profile.Presto ||
		profile.Hive ||
		profile.Hadoop ||
		profile.Spark ||
		profile.S3A ||
		strings.Contains(lowerUA, "app/trino")

	profile.AWSCLI = strings.Contains(lowerUA, "aws-cli/")

	profile.MinIO = strings.Contains(lowerUA, "minio")

	profile.Browser = strings.Contains(lowerUA, "mozilla/") ||
		strings.Contains(lowerUA, "chrome/") ||
		strings.Contains(lowerUA, "safari/") ||
		strings.Contains(lowerUA, "firefox/")

	return profile
}
