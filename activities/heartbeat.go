package activities

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
)

const remoteHeartbeatInterval = 10 * time.Second

// startRemoteHeartbeat keeps long provider and tool calls visible to Temporal.
// Local activities deliberately skip this path because they do not support
// server heartbeats and are governed by their start-to-close timeout instead.
func startRemoteHeartbeat(ctx context.Context, operation string) func() {
	if !activity.IsActivity(ctx) || activity.GetInfo(ctx).IsLocalActivity {
		return func() {}
	}
	activity.RecordHeartbeat(ctx, operation)
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(remoteHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, operation)
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}
