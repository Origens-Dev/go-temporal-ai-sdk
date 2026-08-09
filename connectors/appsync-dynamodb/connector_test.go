package appsyncdynamodb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

func TestConnectorImplementsV2Contract(t *testing.T) {
	var _ updates.Connector = (*Connector)(nil)
}

func TestPreviewManifestTracksSucceededAndFailedAttempts(t *testing.T) {
	succeeded := previewManifestFromEnd(updates.PreviewEndEvent{
		BaseEvent:  updates.BaseEvent{ProtocolVersion: 2, Type: updates.EventTypePreviewEnd, EventID: "e1", StreamID: "s1", OccurredAt: 10},
		PreviewRef: updates.PreviewRef{AttemptID: "a1", TargetRecordID: "message:1", Lane: updates.LaneText, Sequence: 2},
		Outcome:    updates.PreviewOutcomeSucceeded,
		Snapshot:   &updates.Snapshot{Text: "hello"},
	})
	if succeeded.Status != "succeeded" || succeeded.Manifest.Status != updates.PreviewStatusSucceeded || succeeded.Manifest.Snapshot.Text != "hello" {
		t.Fatalf("succeeded = %#v", succeeded)
	}
	failed := previewManifestFromEnd(updates.PreviewEndEvent{
		BaseEvent:  updates.BaseEvent{ProtocolVersion: 2, Type: updates.EventTypePreviewEnd, EventID: "e2", StreamID: "s1", OccurredAt: 11},
		PreviewRef: updates.PreviewRef{AttemptID: "a2", TargetRecordID: "message:1", Lane: updates.LaneText, Sequence: 2},
		Outcome:    updates.PreviewOutcomeFailed,
	})
	if failed.Status != "failed" || failed.Manifest.Status != updates.PreviewStatusActive {
		t.Fatalf("failed = %#v", failed)
	}
}

func TestEventWithCursorPreservesCommonEnvelope(t *testing.T) {
	event := updates.NewRecordUpsertEvent("stream-1", updates.WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage, Status: "completed", Data: map[string]any{"text": "hello"}, UpdatedAt: 10}, "attempt-1", 10)
	stored := eventWithCursor(event, "01JCURSOR").(updates.RecordUpsertEvent)
	if stored.Cursor != "01JCURSOR" || stored.EventID != event.EventID || stored.AcceptedAttemptID != "attempt-1" {
		t.Fatalf("stored = %#v", stored)
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := updates.DecodeEvent(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.EventBase().Cursor != "01JCURSOR" {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestRecordAndEventHashesIncludeExactAcceptedAttempt(t *testing.T) {
	record := updates.WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage, Status: "completed", Data: map[string]any{"text": "hello"}, UpdatedAt: 10}
	if appSyncRecordHash(record, "attempt-1") == appSyncRecordHash(record, "attempt-2") {
		t.Fatal("record hash did not include acceptedAttemptId")
	}
	first := updates.NewRecordUpsertEvent("stream-1", record, "attempt-1", 10)
	second := updates.NewRecordUpsertEvent("stream-1", record, "attempt-2", 10)
	if appSyncEventHash(first) == appSyncEventHash(second) {
		t.Fatal("event hash did not include acceptedAttemptId")
	}
	if appSyncEventHash(first) != appSyncEventHash(eventWithCursor(first, "01JCURSOR")) {
		t.Fatal("event hash included storage cursor")
	}
}

func TestDisabledPublisherStillValidatesEnvelope(t *testing.T) {
	connector := New(Options{Disabled: true})
	err := connector.PublishUpdate(context.Background(), updates.PreviewChunkEvent{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}
