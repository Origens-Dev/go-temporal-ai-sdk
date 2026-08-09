package origens

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

type durableStore struct {
	mu    sync.Mutex
	order []string
}

func (s *durableStore) BeginPreview(context.Context, updates.PreviewBeginEvent) error {
	s.append("durable-preview-begin")
	return nil
}
func (s *durableStore) CheckpointPreview(context.Context, updates.PreviewSnapshotEvent) error {
	s.append("durable-preview-checkpoint")
	return nil
}
func (s *durableStore) EndPreview(context.Context, updates.PreviewEndEvent) error {
	s.append("durable-preview-end")
	return nil
}
func (s *durableStore) UpsertRecord(context.Context, updates.RecordUpsertEvent) error {
	s.append("durable-record")
	return nil
}
func (s *durableStore) EndStream(context.Context, updates.StreamEndEvent) error {
	s.append("durable-terminal")
	return nil
}
func (s *durableStore) append(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.order = append(s.order, value)
}

func serveUnix(t *testing.T, handler http.Handler) (string, func()) {
	t.Helper()
	file, err := os.CreateTemp("", "origens-review-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	return path, func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(path)
	}
}

func recordEvent() updates.RecordUpsertEvent {
	return updates.NewRecordUpsertEvent("conversation-1", updates.WorkflowRecord{
		RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage,
		Status: "completed", Data: map[string]any{"text": "hello"}, UpdatedAt: 1,
	}, "attempt-1", 2)
}

func TestConnectorPublishesOverSlotBoundSocketAfterDurableCommit(t *testing.T) {
	store := &durableStore{}
	var idempotencyKey string
	socket, closeServer := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		store.append("broker")
		if r.URL.Path != IngestPath || r.Header.Get("X-Origens-Agent-Review-Protocol") != Protocol {
			t.Errorf("request path=%q protocol=%q", r.URL.Path, r.Header.Get("X-Origens-Agent-Review-Protocol"))
		}
		idempotencyKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer closeServer()

	connector := New(Options{Durable: store, SocketPath: socket, Timeout: time.Second})
	event := recordEvent()
	if err := connector.UpsertRecord(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if idempotencyKey != event.EventID {
		t.Fatalf("idempotency key = %q", idempotencyKey)
	}
	if len(store.order) != 2 || store.order[0] != "durable-record" || store.order[1] != "broker" {
		t.Fatalf("order = %v", store.order)
	}
}

func TestCommittedRecordSurvivesBrokerFailureAndReportsGap(t *testing.T) {
	store := &durableStore{}
	socket, closeServer := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer closeServer()
	var observed error
	connector := New(Options{
		Durable: store, SocketPath: socket, Timeout: time.Second,
		OnPublicationFailure: func(_ context.Context, _ updates.UpdateEvent, err error) { observed = err },
	})
	if err := connector.UpsertRecord(context.Background(), recordEvent()); err != nil {
		t.Fatalf("post-commit publication error escaped: %v", err)
	}
	if observed == nil || len(store.order) != 1 || store.order[0] != "durable-record" {
		t.Fatalf("observed=%v order=%v", observed, store.order)
	}
}

func TestDirectPreviewPublicationStillReportsTransportFailure(t *testing.T) {
	publisher := NewPublisher(filepath.Join(t.TempDir(), "missing.sock"), 50*time.Millisecond, 0)
	event := updates.PreviewBeginEvent{
		BaseEvent:  updates.BaseEvent{ProtocolVersion: updates.ProtocolVersion, Type: updates.EventTypePreviewBegin, EventID: "e1", StreamID: "s1", OccurredAt: 1},
		PreviewRef: updates.PreviewRef{AttemptID: "a1", TargetRecordID: "m1", Lane: updates.LaneText},
	}
	if err := publisher.PublishUpdate(context.Background(), event); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected socket transport error, got %v", err)
	}
}

func TestDefaultSocketUsesSlotPrivateHostReportMount(t *testing.T) {
	publisher := NewPublisher("", 0, 0)
	if publisher.socketPath != "/run/gobeyond/host/host-report.sock" {
		t.Fatalf("default socket = %q", publisher.socketPath)
	}
}
