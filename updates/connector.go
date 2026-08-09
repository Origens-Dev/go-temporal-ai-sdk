package updates

import "context"

type PreviewStore interface {
	BeginPreview(context.Context, PreviewBeginEvent) error
	CheckpointPreview(context.Context, PreviewSnapshotEvent) error
	EndPreview(context.Context, PreviewEndEvent) error
}

type RecordStore interface {
	UpsertRecord(context.Context, RecordUpsertEvent) error
	EndStream(context.Context, StreamEndEvent) error
}

type LivePublisher interface {
	PublishUpdate(context.Context, UpdateEvent) error
}

type Connector interface {
	PreviewStore
	RecordStore
	LivePublisher
}

type CompositeOptions struct {
	PreviewStore         PreviewStore
	RecordStore          RecordStore
	LivePublisher        LivePublisher
	OnPublicationFailure func(context.Context, UpdateEvent, error)
}

type CompositeConnector struct {
	previews             PreviewStore
	records              RecordStore
	publisher            LivePublisher
	onPublicationFailure func(context.Context, UpdateEvent, error)
}

func NewCompositeConnector(options CompositeOptions) *CompositeConnector {
	return &CompositeConnector{
		previews: options.PreviewStore, records: options.RecordStore, publisher: options.LivePublisher,
		onPublicationFailure: options.OnPublicationFailure,
	}
}

func (c *CompositeConnector) BeginPreview(ctx context.Context, event PreviewBeginEvent) error {
	if c != nil && c.previews != nil {
		if err := c.previews.BeginPreview(ctx, event); err != nil {
			return err
		}
		return c.publishAfterCommit(ctx, event)
	}
	return c.PublishUpdate(ctx, event)
}

func (c *CompositeConnector) CheckpointPreview(ctx context.Context, event PreviewSnapshotEvent) error {
	if c != nil && c.previews != nil {
		if err := c.previews.CheckpointPreview(ctx, event); err != nil {
			return err
		}
		return c.publishAfterCommit(ctx, event)
	}
	return c.PublishUpdate(ctx, event)
}

func (c *CompositeConnector) EndPreview(ctx context.Context, event PreviewEndEvent) error {
	if c != nil && c.previews != nil {
		if err := c.previews.EndPreview(ctx, event); err != nil {
			return err
		}
		return c.publishAfterCommit(ctx, event)
	}
	return c.PublishUpdate(ctx, event)
}

func (c *CompositeConnector) UpsertRecord(ctx context.Context, event RecordUpsertEvent) error {
	if c != nil && c.records != nil {
		if err := c.records.UpsertRecord(ctx, event); err != nil {
			return err
		}
		return c.publishAfterCommit(ctx, event)
	}
	return c.PublishUpdate(ctx, event)
}

func (c *CompositeConnector) EndStream(ctx context.Context, event StreamEndEvent) error {
	if c != nil && c.records != nil {
		if err := c.records.EndStream(ctx, event); err != nil {
			return err
		}
		return c.publishAfterCommit(ctx, event)
	}
	return c.PublishUpdate(ctx, event)
}

func (c *CompositeConnector) PublishUpdate(ctx context.Context, event UpdateEvent) error {
	if c == nil || c.publisher == nil {
		return nil
	}
	return c.publisher.PublishUpdate(ctx, event)
}

// publishAfterCommit deliberately does not return a live publication error.
// The durable store has already committed, so surfacing the publisher error
// could make an activity retry provider/tool work or a customer-owned write.
// OnPublicationFailure is the explicit gap and health signal instead.
func (c *CompositeConnector) publishAfterCommit(ctx context.Context, event UpdateEvent) error {
	if err := c.PublishUpdate(ctx, event); err != nil {
		if c != nil && c.onPublicationFailure != nil {
			c.onPublicationFailure(ctx, event, err)
		}
	}
	return nil
}

type NoopConnector struct{}

func (NoopConnector) BeginPreview(context.Context, PreviewBeginEvent) error         { return nil }
func (NoopConnector) CheckpointPreview(context.Context, PreviewSnapshotEvent) error { return nil }
func (NoopConnector) EndPreview(context.Context, PreviewEndEvent) error             { return nil }
func (NoopConnector) UpsertRecord(context.Context, RecordUpsertEvent) error         { return nil }
func (NoopConnector) EndStream(context.Context, StreamEndEvent) error               { return nil }
func (NoopConnector) PublishUpdate(context.Context, UpdateEvent) error              { return nil }
