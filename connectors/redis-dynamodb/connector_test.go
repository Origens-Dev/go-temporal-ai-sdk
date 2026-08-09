package redisdynamodb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

func TestConnectorImplementsV2Contract(t *testing.T) {
	var _ updates.Connector = (*Connector)(nil)
}

func TestEventWithCursorPreservesCommonEnvelope(t *testing.T) {
	event := updates.NewStreamEndEvent("stream-1", updates.StreamOutcomeCompleted, "", 10)
	stored := redisEventWithCursor(event, "01JCURSOR").(updates.StreamEndEvent)
	if stored.Cursor != "01JCURSOR" || stored.EventID != event.EventID || stored.Outcome != updates.StreamOutcomeCompleted {
		t.Fatalf("stored = %#v", stored)
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := updates.DecodeEvent(payload)
	if err != nil || decoded.EventBase().Cursor != "01JCURSOR" {
		t.Fatalf("decoded = %#v, err = %v", decoded, err)
	}
}

func TestRecordHashChangesWithPayload(t *testing.T) {
	first := updates.WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage, Status: "completed", Data: map[string]any{"text": "one"}, UpdatedAt: 10}
	second := first
	second.Data = map[string]any{"text": "two"}
	if recordHash(first, "attempt-1") == recordHash(second, "attempt-1") {
		t.Fatal("record hash did not include payload")
	}
}

func TestRecordAndEventHashesIncludeExactAcceptedAttempt(t *testing.T) {
	record := updates.WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage, Status: "completed", Data: map[string]any{"text": "hello"}, UpdatedAt: 10}
	if recordHash(record, "attempt-1") == recordHash(record, "attempt-2") {
		t.Fatal("record hash did not include acceptedAttemptId")
	}
	first := updates.NewRecordUpsertEvent("stream-1", record, "attempt-1", 10)
	second := updates.NewRecordUpsertEvent("stream-1", record, "attempt-2", 10)
	if redisEventHash(first) == redisEventHash(second) {
		t.Fatal("event hash did not include acceptedAttemptId")
	}
	if redisEventHash(first) != redisEventHash(redisEventWithCursor(first, "01JCURSOR")) {
		t.Fatal("event hash included storage cursor")
	}
}

func TestDisabledPublisherStillValidatesEnvelope(t *testing.T) {
	connector := New(Options{Disabled: true})
	if err := connector.PublishUpdate(context.Background(), updates.PreviewChunkEvent{}); err == nil {
		t.Fatal("expected validation error")
	}
}
