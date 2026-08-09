package redisdynamodb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestDynamoDBResolverReturnsTypedStreamNotFound(t *testing.T) {
	resolver := NewDynamoDBResolver(DynamoDBResolverOptions{
		DynamoDB:  emptyQueryDynamoDBClient(),
		TableName: "streams",
	})

	_, err := resolver.ResolveStream(context.Background(), "missing-stream")
	if !errors.Is(err, updates.ErrStreamNotFound) {
		t.Fatalf("err = %v, want stream not found", err)
	}
	var notFound *updates.StreamNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("err = %T, want StreamNotFoundError", err)
	}
	if notFound.StreamID != "missing-stream" {
		t.Fatalf("stream id = %q", notFound.StreamID)
	}
}

func emptyQueryDynamoDBClient() *dynamodb.Client {
	return dynamodb.NewFromConfig(aws.Config{
		Region:      "us-west-2",
		Credentials: staticCredentials{},
		HTTPClient: httpClientFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
				Body:       io.NopCloser(strings.NewReader(`{"Items":[],"Count":0,"ScannedCount":0}`)),
			}, nil
		}),
	})
}

type staticCredentials struct{}

func (staticCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		Source:          "test",
	}, nil
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}
