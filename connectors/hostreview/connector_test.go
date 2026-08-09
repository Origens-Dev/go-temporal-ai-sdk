package hostreview

import "testing"

func TestCompatibilitySurfaceUsesHostedDefaults(t *testing.T) {
	if DefaultSocketPath != "/run/gobeyond/host/host-report.sock" {
		t.Fatalf("default socket = %q", DefaultSocketPath)
	}
	if IngestPath != "/v1/agent-review/events" || Protocol == "" {
		t.Fatalf("ingest path=%q protocol=%q", IngestPath, Protocol)
	}
}
