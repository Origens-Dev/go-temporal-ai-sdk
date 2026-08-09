package updates

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
)

func TestFrozenEventFixturesRoundTrip(t *testing.T) {
	payload, err := os.ReadFile("../protocol/v2/events.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []json.RawMessage
	if err := json.Unmarshal(payload, &fixtures); err != nil {
		t.Fatal(err)
	}
	wantTypes := []EventType{EventTypePreviewBegin, EventTypePreviewChunk, EventTypePreviewSnapshot, EventTypePreviewEnd, EventTypeRecordUpsert, EventTypeRecordUpsert, EventTypeRecordUpsert, EventTypeRecordUpsert, EventTypeRecordUpsert, EventTypeStreamEnd}
	if len(fixtures) != len(wantTypes) {
		t.Fatalf("fixtures = %d, want %d", len(fixtures), len(wantTypes))
	}
	for index, fixture := range fixtures {
		event, err := DecodeEvent(fixture)
		if err != nil {
			t.Fatalf("fixture %d: %v", index, err)
		}
		if event.EventBase().Type != wantTypes[index] {
			t.Fatalf("fixture %d type = %q", index, event.EventBase().Type)
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if !jsonEquivalent(fixture, encoded) {
			t.Fatalf("fixture %d did not round trip\nwant: %s\n got: %s", index, fixture, encoded)
		}
	}
}

func TestFrozenReplayFixtureDecodes(t *testing.T) {
	payload, err := os.ReadFile("../protocol/v2/replay.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var replay ReplayResponse
	if err := decoder.Decode(&replay); err != nil {
		t.Fatal(err)
	}
	if replay.ProtocolVersion != ProtocolVersion || replay.StreamID != "stream-1" || replay.Cursor == "" {
		t.Fatalf("replay = %#v", replay)
	}
	for index, raw := range replay.Events {
		if _, err := DecodeEvent(raw); err != nil {
			t.Fatalf("event %d: %v", index, err)
		}
	}
	for _, preview := range replay.Previews {
		if err := validatePreview(PreviewRef{AttemptID: preview.AttemptID, TargetRecordID: preview.TargetRecordID, Lane: preview.Lane, Sequence: preview.Sequence, Scope: preview.Scope}); err != nil {
			t.Fatal(err)
		}
	}
	for _, record := range replay.Records {
		if err := validateRecord(record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFrozenFixturesUseCamelFieldsAndKebabDiscriminants(t *testing.T) {
	payload, err := os.ReadFile("../protocol/v2/events.json")
	if err != nil {
		t.Fatal(err)
	}
	var values []any
	if err := json.Unmarshal(payload, &values); err != nil {
		t.Fatal(err)
	}
	var visit func(any)
	visit = func(value any) {
		switch typed := value.(type) {
		case []any:
			for _, item := range typed {
				visit(item)
			}
		case map[string]any:
			for key, item := range typed {
				if strings.Contains(key, "_") || (len(key) > 0 && key[0] >= 'A' && key[0] <= 'Z') {
					t.Errorf("non-camel JSON field %q", key)
				}
				visit(item)
			}
		}
	}
	visit(values)
	for _, eventType := range []EventType{EventTypePreviewBegin, EventTypePreviewChunk, EventTypePreviewSnapshot, EventTypePreviewEnd, EventTypeRecordUpsert, EventTypeStreamEnd} {
		if strings.Contains(string(eventType), "_") || strings.ContainsAny(string(eventType), "ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			t.Errorf("non-kebab event type %q", eventType)
		}
	}
}

func TestValidateEventRejectsConcreteTypeDiscriminantMismatch(t *testing.T) {
	event := NewRecordUpsertEvent("stream-1", WorkflowRecord{
		RecordID: "message:1", RecordVersion: 1, Kind: RecordKindMessage,
		Status: "completed", Data: map[string]any{}, UpdatedAt: 1,
	}, "", 1)
	event.Type = EventTypePreviewBegin
	if err := ValidateEvent(event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("err = %v, want ErrInvalidEvent", err)
	}
}

func TestDecodeEventRejectsSchemaIncompatibleFields(t *testing.T) {
	for _, payload := range []string{
		`{"protocolVersion":2,"type":"preview-begin","eventId":"e","streamId":"s","occurredAt":1,"attemptId":"a","targetRecordId":"r","lane":"text","sequence":0,"unknown":true}`,
		`{"protocolVersion":2,"type":"preview-begin","eventId":"e","cursor":"","streamId":"s","occurredAt":1,"attemptId":"a","targetRecordId":"r","lane":"text","sequence":0}`,
		`{"protocolVersion":2,"type":"record-upsert","eventId":"e","streamId":"s","occurredAt":1,"acceptedAttemptId":"","record":{"recordId":"r","recordVersion":1,"kind":"message","status":"completed","data":{},"updatedAt":1}}`,
		`{"protocolVersion":2,"type":"preview-begin","eventId":"e","streamId":"s","occurredAt":1,"attemptId":"a","targetRecordId":"r","lane":"text","sequence":0,"scope":{"stepNumber":-1}}`,
	} {
		if event, err := DecodeEvent([]byte(payload)); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("DecodeEvent(%s) = %#v, %v; want ErrInvalidEvent", payload, event, err)
		}
	}
}

func TestRelayKeepsAttemptsSeparateAndReturnsExactReceipts(t *testing.T) {
	connector := &memoryConnector{}
	now := time.UnixMilli(1000)
	relay := NewRelayWithClock(connector, Options{Visible: true, StreamID: "stream-1", AttemptID: "attempt-1", TargetRecordID: "message:1"}, func() time.Time { return now })
	if err := relay.Accept(context.Background(), ai.StreamPart{Type: "stream-start"}); err != nil {
		t.Fatal(err)
	}
	if err := relay.Accept(context.Background(), ai.StreamPart{Type: "text-delta", TextDelta: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := relay.Accept(context.Background(), ai.StreamPart{Type: "reasoning-delta", ReasoningDelta: "thinking"}); err != nil {
		t.Fatal(err)
	}
	if err := relay.Succeed(context.Background()); err != nil {
		t.Fatal(err)
	}
	receipts := relay.Receipts()
	if len(receipts) != 2 {
		t.Fatalf("receipts = %#v", receipts)
	}
	byLane := map[Lane]PreviewReceipt{}
	for _, receipt := range receipts {
		byLane[receipt.Lane] = receipt
	}
	if byLane[LaneText].TargetRecordID != "message:1" || byLane[LaneReasoning].TargetRecordID != "message:1:reasoning" {
		t.Fatalf("receipts = %#v", receipts)
	}
	if byLane[LaneText].AttemptID == byLane[LaneReasoning].AttemptID || byLane[LaneText].Outcome != PreviewOutcomeSucceeded {
		t.Fatalf("receipts = %#v", receipts)
	}
}

func TestRelayCarriesAgentAndConversationIdentity(t *testing.T) {
	connector := &memoryConnector{}
	relay := NewRelayWithClock(connector, Options{
		Visible: true, StreamID: "run-1", ConversationID: "conversation-1",
		Scope: Scope{AgentID: "support"}, AttemptID: "attempt-1", TargetRecordID: "message:1",
	}, func() time.Time { return time.UnixMilli(1000) })
	if err := relay.Accept(context.Background(), ai.StreamPart{Type: "stream-start"}); err != nil {
		t.Fatal(err)
	}
	if len(connector.begins) != 1 {
		t.Fatalf("begins = %d", len(connector.begins))
	}
	base := connector.begins[0].EventBase()
	if base.StreamID != "run-1" || base.ConversationID != "conversation-1" || base.AgentID != "support" {
		t.Fatalf("base = %+v", base)
	}
}

func TestCompositePersistsBeforePublishing(t *testing.T) {
	store := &orderedStore{}
	publisher := &orderedPublisher{order: &store.order}
	connector := NewCompositeConnector(CompositeOptions{RecordStore: store, LivePublisher: publisher})
	event := NewRecordUpsertEvent("stream-1", WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: RecordKindMessage, Status: "completed", Data: map[string]any{"text": "hello"}, UpdatedAt: 1}, "attempt-1", 1)
	if err := connector.UpsertRecord(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.order, []string{"persist-record", "publish"}) {
		t.Fatalf("order = %#v", store.order)
	}
}

func TestCompositeDoesNotRetryCommittedRecordForPublisherFailure(t *testing.T) {
	want := errors.New("publisher unavailable")
	store := &orderedStore{}
	publisher := &orderedPublisher{order: &store.order, err: want}
	var observed error
	connector := NewCompositeConnector(CompositeOptions{
		RecordStore: store, LivePublisher: publisher,
		OnPublicationFailure: func(_ context.Context, _ UpdateEvent, err error) { observed = err },
	})
	event := NewRecordUpsertEvent("stream-1", WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: RecordKindMessage, Status: "completed", Data: map[string]any{"text": "hello"}, UpdatedAt: 1}, "attempt-1", 1)
	if err := connector.UpsertRecord(context.Background(), event); err != nil {
		t.Fatalf("publisher failure after durable commit must not escape: %v", err)
	}
	if !errors.Is(observed, want) || !reflect.DeepEqual(store.order, []string{"persist-record", "publish"}) {
		t.Fatalf("observed=%v order=%v", observed, store.order)
	}
}

func TestCompositeWithoutDurableStoreReturnsPublisherFailure(t *testing.T) {
	want := errors.New("publisher unavailable")
	publisher := &orderedPublisher{order: &[]string{}, err: want}
	connector := NewCompositeConnector(CompositeOptions{LivePublisher: publisher})
	event := NewRecordUpsertEvent("stream-1", WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: RecordKindMessage, Status: "completed", Data: map[string]any{}, UpdatedAt: 1}, "", 1)
	if err := connector.UpsertRecord(context.Background(), event); !errors.Is(err, want) {
		t.Fatalf("err = %v, want publisher failure", err)
	}
}

func TestBestEffortDoesNotHideArbitraryPublisherFailures(t *testing.T) {
	want := errors.New("publisher unavailable")
	connector := &memoryConnector{publishErr: want}
	relay := NewRelay(connector, Options{Visible: true, StreamID: "stream-1", FailurePolicy: FailurePolicyBestEffort})
	err := relay.Accept(context.Background(), ai.StreamPart{Type: "stream-start"})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

func jsonEquivalent(left, right []byte) bool {
	decode := func(data []byte) any {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value any
		_ = decoder.Decode(&value)
		return value
	}
	return reflect.DeepEqual(decode(left), decode(right))
}

type memoryConnector struct {
	begins     []PreviewBeginEvent
	snapshots  []PreviewSnapshotEvent
	ends       []PreviewEndEvent
	published  []UpdateEvent
	publishErr error
}

func (c *memoryConnector) BeginPreview(_ context.Context, event PreviewBeginEvent) error {
	c.begins = append(c.begins, event)
	return c.publishErr
}
func (c *memoryConnector) CheckpointPreview(_ context.Context, event PreviewSnapshotEvent) error {
	c.snapshots = append(c.snapshots, event)
	return c.publishErr
}
func (c *memoryConnector) EndPreview(_ context.Context, event PreviewEndEvent) error {
	c.ends = append(c.ends, event)
	return c.publishErr
}
func (c *memoryConnector) UpsertRecord(context.Context, RecordUpsertEvent) error { return nil }
func (c *memoryConnector) EndStream(context.Context, StreamEndEvent) error       { return nil }
func (c *memoryConnector) PublishUpdate(_ context.Context, event UpdateEvent) error {
	c.published = append(c.published, event)
	return c.publishErr
}

type orderedStore struct{ order []string }

func (s *orderedStore) UpsertRecord(context.Context, RecordUpsertEvent) error {
	s.order = append(s.order, "persist-record")
	return nil
}
func (s *orderedStore) EndStream(context.Context, StreamEndEvent) error {
	s.order = append(s.order, "persist-terminal")
	return nil
}

type orderedPublisher struct {
	order *[]string
	err   error
}

func (p *orderedPublisher) PublishUpdate(context.Context, UpdateEvent) error {
	*p.order = append(*p.order, "publish")
	return p.err
}
