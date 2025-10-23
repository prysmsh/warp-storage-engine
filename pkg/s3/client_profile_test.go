package s3

import (
	"net/http/httptest"
	"testing"
)

func TestDetectClientProfileFromRequest(t *testing.T) {
	req := httptest.NewRequest("GET", "/bucket/key?x-id=GetObject", nil)
	req.Header.Set("User-Agent", "aws-sdk-java/2.30.12 md/io#sync md/http#Apache ua/2.1 app/Trino")
	req.Header.Set("x-amz-sdk-request", "attempt=1")

	profile := DetectClientProfile(req)

	if !profile.JavaSDK {
		t.Fatalf("expected JavaSDK to be true")
	}
	if !profile.Trino {
		t.Fatalf("expected Trino flag to be true")
	}
	if !profile.SDKv2 {
		t.Fatalf("expected SDKv2 flag to be true")
	}
	if profile.AWSCLI {
		t.Fatalf("did not expect AWSCLI flag to be true")
	}
}

func TestWithClientProfileStoresInContext(t *testing.T) {
	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("User-Agent", "aws-cli/2.16.6 Python/3.11.6 Linux/6.6.0")

	req, profile := WithClientProfile(req)

	if !profile.AWSCLI {
		t.Fatalf("expected AWSCLI to be true")
	}

	ctxProfile := GetClientProfile(req)
	if !ctxProfile.AWSCLI {
		t.Fatalf("expected context profile to have AWSCLI flag true")
	}
	if ctxProfile.UserAgent != profile.UserAgent {
		t.Fatalf("expected stored profile to match original")
	}
}
