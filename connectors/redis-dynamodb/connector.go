package redisdynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/redis/go-redis/v9"
)

type Connector struct {
	options Options
	ddb     *dynamodb.Client
	redis   redis.UniversalClient
}

func New(options Options) *Connector {
	ddb := options.DynamoDB
	if ddb == nil {
		ddb = dynamodb.NewFromConfig(options.AWSConfig)
	}
	return &Connector{options: options, ddb: ddb, redis: options.Redis}
}

func (c *Connector) BeginPreview(ctx context.Context, event updates.PreviewBeginEvent) error {
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	if err := c.putPreview(ctx, event.StreamID, event.AttemptID, event.TargetRecordID, event.Lane, event.Sequence, "active", updates.Snapshot{}, event.Scope, event.OccurredAt); err != nil {
		return err
	}
	return c.PublishUpdate(ctx, event)
}

func (c *Connector) CheckpointPreview(ctx context.Context, event updates.PreviewSnapshotEvent) error {
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	if err := c.putPreview(ctx, event.StreamID, event.AttemptID, event.TargetRecordID, event.Lane, event.Sequence, "active", event.Snapshot, event.Scope, event.OccurredAt); err != nil {
		return err
	}
	return c.PublishUpdate(ctx, event)
}

func (c *Connector) EndPreview(ctx context.Context, event updates.PreviewEndEvent) error {
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	snapshot := updates.Snapshot{}
	if event.Snapshot != nil {
		snapshot = *event.Snapshot
	}
	if err := c.putPreview(ctx, event.StreamID, event.AttemptID, event.TargetRecordID, event.Lane, event.Sequence, string(event.Outcome), snapshot, event.Scope, event.OccurredAt); err != nil {
		return err
	}
	return c.PublishUpdate(ctx, event)
}

func (c *Connector) UpsertRecord(ctx context.Context, event updates.RecordUpsertEvent) error {
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	if err := c.validateAcceptedPreviewTarget(ctx, event); err != nil {
		return err
	}
	current, err := c.putCurrentRecord(ctx, event)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}
	stored, err := c.persistDurableEvent(ctx, event)
	if err != nil {
		return err
	}
	if event.AcceptedAttemptID != "" {
		if err := c.markPreviewAccepted(ctx, event.StreamID, event.AcceptedAttemptID); err != nil {
			return err
		}
	}
	return c.PublishUpdate(ctx, stored)
}

func (c *Connector) EndStream(ctx context.Context, event updates.StreamEndEvent) error {
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	stored, err := c.persistDurableEvent(ctx, event)
	if err != nil {
		return err
	}
	if err := c.putTerminal(ctx, stored); err != nil {
		return err
	}
	return c.PublishUpdate(ctx, stored)
}

func (c *Connector) PublishUpdate(ctx context.Context, event updates.UpdateEvent) error {
	if err := updates.ValidateEvent(event); err != nil {
		return err
	}
	if c == nil || c.options.Disabled {
		return nil
	}
	if c.redis == nil {
		return fmt.Errorf("redis client is required")
	}
	base := event.EventBase()
	ref, err := c.resolve(ctx, base.StreamID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	switch c.options.mode() {
	case ModeStream:
		return c.xadd(ctx, ref, base, payload)
	case ModeBoth:
		if err := c.redis.Publish(ctx, ref.Channel, payload).Err(); err != nil {
			return err
		}
		return c.xadd(ctx, ref, base, payload)
	default:
		return c.redis.Publish(ctx, ref.Channel, payload).Err()
	}
}

func (c *Connector) xadd(ctx context.Context, ref StreamRef, base updates.BaseEvent, payload []byte) error {
	args := &redis.XAddArgs{Stream: ref.RedisStream, Values: map[string]any{"eventId": base.EventID, "cursor": base.Cursor, "streamId": base.StreamID, "payload": string(payload)}}
	if c.options.MaxStreamLength > 0 {
		args.MaxLen = c.options.MaxStreamLength
		args.Approx = true
	}
	return c.redis.XAdd(ctx, args).Err()
}

func (c *Connector) putPreview(ctx context.Context, streamID, attemptID, targetRecordID string, lane updates.Lane, sequence int, status string, snapshot updates.Snapshot, scope updates.Scope, updatedAt int64) error {
	ref, err := c.resolve(ctx, streamID)
	if err != nil {
		return err
	}
	key := c.stableKey("preview#"+streamID+"#"+attemptID, c.options.stateSortKey())
	existing, err := c.getItem(ctx, key)
	if err != nil {
		return err
	}
	if target := stringField(existing, "targetRecordId"); target != "" && target != targetRecordID {
		return fmt.Errorf("%w: preview %s changed targetRecordId from %s to %s", updates.ErrEventConflict, attemptID, target, targetRecordID)
	}
	if currentLane := stringField(existing, "lane"); currentLane != "" && currentLane != string(lane) {
		return fmt.Errorf("%w: preview %s changed lane from %s to %s", updates.ErrEventConflict, attemptID, currentLane, lane)
	}
	if current, ok := redisIntField(existing, "sequence"); ok && current > sequence {
		return nil
	}
	if current := stringField(existing, "status"); current == "accepted" || current == "failed" || current == "canceled" {
		return nil
	}
	item := c.baseStableItem(ref, key, updatedAt)
	item["entityType"] = c.options.previewEntityType()
	item["protocolVersion"] = updates.ProtocolVersion
	item["streamId"] = streamID
	item["attemptId"] = attemptID
	item["targetRecordId"] = targetRecordID
	item["lane"] = lane
	item["status"] = status
	item["sequence"] = sequence
	item["snapshot"] = snapshot
	item["scope"] = scope
	item["preview"] = updates.PreviewManifest{AttemptID: attemptID, TargetRecordID: targetRecordID, Lane: lane, Status: updates.PreviewStatus(status), Sequence: sequence, Snapshot: snapshot, Scope: scope, UpdatedAt: updatedAt}
	item[c.options.previewStreamKeyName()] = streamID
	item[c.options.previewUpdatedAtKeyName()] = updatedAt
	item["expiresAt"] = time.Now().Add(c.options.ttl()).Unix()
	av, err := marshalMap(cleanMap(item))
	if err != nil {
		return err
	}
	values, err := marshalMap(map[string]any{":sequence": sequence, ":status": status, ":active": "active"})
	if err != nil {
		return err
	}
	_, err = c.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.options.TableName), Item: av,
		ConditionExpression:      aws.String("attribute_not_exists(#sequence) OR (#sequence < :sequence AND #status = :active) OR (#sequence = :sequence AND #status = :status)"),
		ExpressionAttributeNames: map[string]string{"#sequence": "sequence", "#status": "status"}, ExpressionAttributeValues: values,
	})
	var conditional *types.ConditionalCheckFailedException
	if errors.As(err, &conditional) {
		return nil
	}
	return err
}

func (c *Connector) markPreviewAccepted(ctx context.Context, streamID, attemptID string) error {
	key := c.stableKey("preview#"+streamID+"#"+attemptID, c.options.stateSortKey())
	values, err := marshalMap(map[string]any{":status": "accepted", ":updatedAt": time.Now().UnixMilli(), ":expiresAt": time.Now().Add(c.options.ttl()).Unix()})
	if err != nil {
		return err
	}
	_, err = c.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(c.options.TableName), Key: key,
		UpdateExpression:         aws.String("SET #status = :status, updatedAt = :updatedAt, expiresAt = :expiresAt"),
		ConditionExpression:      aws.String("attribute_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{"#pk": c.options.partitionKeyName(), "#status": "status"}, ExpressionAttributeValues: values,
	})
	var conditional *types.ConditionalCheckFailedException
	if errors.As(err, &conditional) {
		return nil
	}
	return err
}

func (c *Connector) validateAcceptedPreviewTarget(ctx context.Context, event updates.RecordUpsertEvent) error {
	if event.AcceptedAttemptID == "" {
		return nil
	}
	key := c.stableKey("preview#"+event.StreamID+"#"+event.AcceptedAttemptID, c.options.stateSortKey())
	existing, err := c.getItem(ctx, key)
	if err != nil || len(existing) == 0 {
		return err
	}
	if target := stringField(existing, "targetRecordId"); target != "" && target != event.Record.RecordID {
		return fmt.Errorf("%w: accepted attempt %s targets %s, not %s", updates.ErrRecordConflict, event.AcceptedAttemptID, target, event.Record.RecordID)
	}
	return nil
}

// putCurrentRecord reports whether event is current and should continue to the
// durable-event/publish stages. Stale versions are successful no-ops.
func (c *Connector) putCurrentRecord(ctx context.Context, event updates.RecordUpsertEvent) (bool, error) {
	ref, err := c.resolve(ctx, event.StreamID)
	if err != nil {
		return false, err
	}
	key := c.stableKey("record#"+event.StreamID+"#"+event.Record.RecordID, c.options.stateSortKey())
	existing, err := c.getItem(ctx, key)
	if err != nil {
		return false, err
	}
	payloadHash := recordHash(event.Record, event.AcceptedAttemptID)
	if currentVersion, ok := redisIntField(existing, "recordVersion"); ok {
		if currentVersion > event.Record.RecordVersion {
			return false, nil
		}
		if currentVersion == event.Record.RecordVersion {
			if stringField(existing, "eventId") == event.EventID && stringField(existing, "payloadHash") == payloadHash {
				return true, nil
			}
			return false, fmt.Errorf("%w: %s version %d", updates.ErrRecordConflict, event.Record.RecordID, event.Record.RecordVersion)
		}
	}
	item := c.baseStableItem(ref, key, event.Record.UpdatedAt)
	item["entityType"] = c.options.recordEntityType()
	item["protocolVersion"] = updates.ProtocolVersion
	item["streamId"] = event.StreamID
	item["recordId"] = event.Record.RecordID
	item["recordVersion"] = event.Record.RecordVersion
	item["eventId"] = event.EventID
	item["payloadHash"] = payloadHash
	item["record"] = event.Record
	item[c.options.recordStreamKeyName()] = event.StreamID
	item[c.options.recordUpdatedAtKeyName()] = event.Record.UpdatedAt
	av, err := marshalMap(cleanMap(item))
	if err != nil {
		return false, err
	}
	values, err := marshalMap(map[string]any{":version": event.Record.RecordVersion, ":eventId": event.EventID, ":payloadHash": payloadHash})
	if err != nil {
		return false, err
	}
	_, err = c.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.options.TableName), Item: av,
		ConditionExpression:      aws.String("attribute_not_exists(#version) OR #version < :version OR (#version = :version AND #eventId = :eventId AND #payloadHash = :payloadHash)"),
		ExpressionAttributeNames: map[string]string{"#version": "recordVersion", "#eventId": "eventId", "#payloadHash": "payloadHash"}, ExpressionAttributeValues: values,
	})
	var conditional *types.ConditionalCheckFailedException
	if !errors.As(err, &conditional) {
		return err == nil, err
	}
	latest, getErr := c.getItem(ctx, key)
	if getErr != nil {
		return false, getErr
	}
	latestVersion, _ := redisIntField(latest, "recordVersion")
	if latestVersion > event.Record.RecordVersion {
		return false, nil
	}
	if latestVersion == event.Record.RecordVersion && stringField(latest, "eventId") == event.EventID && stringField(latest, "payloadHash") == payloadHash {
		return true, nil
	}
	return false, fmt.Errorf("%w: %s version %d", updates.ErrRecordConflict, event.Record.RecordID, event.Record.RecordVersion)
}

func (c *Connector) persistDurableEvent(ctx context.Context, event updates.UpdateEvent) (updates.UpdateEvent, error) {
	base := event.EventBase()
	eventHash := redisEventHash(event)
	ref, err := c.resolve(ctx, base.StreamID)
	if err != nil {
		return nil, err
	}
	key := c.stableKey("event#"+base.StreamID+"#"+base.EventID, c.options.stateSortKey())
	existing, err := c.getItem(ctx, key)
	if err != nil {
		return nil, err
	}
	if cursor := stringField(existing, "cursor"); cursor != "" {
		if storedHash := redisStoredEventHash(existing); storedHash == "" || storedHash != eventHash {
			return nil, fmt.Errorf("%w: %s", updates.ErrEventConflict, base.EventID)
		}
		return redisEventWithCursor(event, cursor), nil
	}
	cursor := newEventID()
	stored := redisEventWithCursor(event, cursor)
	item := c.baseStableItem(ref, key, base.OccurredAt)
	item["entityType"] = c.options.eventEntityType()
	item["protocolVersion"] = updates.ProtocolVersion
	item["streamId"] = base.StreamID
	item["eventId"] = base.EventID
	item["eventHash"] = eventHash
	item["cursor"] = cursor
	item["event"] = stored
	item["updateEvent"] = stored
	item[c.options.eventStreamKeyName()] = base.StreamID
	item[c.options.eventCursorKeyName()] = cursor
	item["expiresAt"] = time.Now().Add(c.options.ttl()).Unix()
	if err := c.putItem(ctx, item, "attribute_not_exists(#pk)"); err != nil {
		var conditional *types.ConditionalCheckFailedException
		if !errors.As(err, &conditional) {
			return nil, err
		}
		existing, getErr := c.getItem(ctx, key)
		if getErr != nil {
			return nil, getErr
		}
		cursor = stringField(existing, "cursor")
		if cursor == "" {
			return nil, fmt.Errorf("durable event %q exists without cursor", base.EventID)
		}
		if storedHash := redisStoredEventHash(existing); storedHash == "" || storedHash != eventHash {
			return nil, fmt.Errorf("%w: %s", updates.ErrEventConflict, base.EventID)
		}
		stored = redisEventWithCursor(event, cursor)
	}
	return stored, nil
}

func (c *Connector) putTerminal(ctx context.Context, event updates.UpdateEvent) error {
	terminal, ok := event.(updates.StreamEndEvent)
	if !ok {
		if pointer, pointerOK := event.(*updates.StreamEndEvent); pointerOK {
			terminal = *pointer
		} else {
			return fmt.Errorf("invalid terminal event %T", event)
		}
	}
	ref, err := c.resolve(ctx, terminal.StreamID)
	if err != nil {
		return err
	}
	key := c.stableKey("terminal#"+terminal.StreamID, c.options.stateSortKey())
	item := c.baseStableItem(ref, key, terminal.OccurredAt)
	item["entityType"] = c.options.terminalEntityType()
	item["protocolVersion"] = updates.ProtocolVersion
	item["streamId"] = terminal.StreamID
	item["terminal"] = terminal
	return c.putItem(ctx, item, "")
}

func redisEventWithCursor(event updates.UpdateEvent, cursor string) updates.UpdateEvent {
	switch value := event.(type) {
	case updates.RecordUpsertEvent:
		value.Cursor = cursor
		return value
	case *updates.RecordUpsertEvent:
		copy := *value
		copy.Cursor = cursor
		return copy
	case updates.StreamEndEvent:
		value.Cursor = cursor
		return value
	case *updates.StreamEndEvent:
		copy := *value
		copy.Cursor = cursor
		return copy
	default:
		return event
	}
}

func recordHash(record updates.WorkflowRecord, acceptedAttemptID string) string {
	payload, _ := json.Marshal(struct {
		AcceptedAttemptID string                 `json:"acceptedAttemptId,omitempty"`
		Record            updates.WorkflowRecord `json:"record"`
	}{AcceptedAttemptID: acceptedAttemptID, Record: record})
	return redisPayloadHash(payload)
}

func redisEventHash(event updates.UpdateEvent) string {
	payload, _ := json.Marshal(redisEventWithCursor(event, ""))
	return redisPayloadHash(payload)
}

func redisStoredEventHash(item map[string]any) string {
	if hash := stringField(item, "eventHash"); hash != "" {
		return hash
	}
	raw, ok := item["event"].(map[string]any)
	if !ok {
		return ""
	}
	copy := make(map[string]any, len(raw))
	for key, value := range raw {
		copy[key] = value
	}
	delete(copy, "cursor")
	payload, _ := json.Marshal(copy)
	return redisPayloadHash(payload)
}

func redisPayloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (c *Connector) stableKey(id string, sort int64) map[string]types.AttributeValue {
	key, _ := marshalMap(map[string]any{c.options.partitionKeyName(): id, c.options.sortKeyName(): sort})
	return key
}

func (c *Connector) getItem(ctx context.Context, key map[string]types.AttributeValue) (map[string]any, error) {
	output, err := c.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(c.options.TableName), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil || len(output.Item) == 0 {
		return nil, err
	}
	var item map[string]any
	if err := attributevalue.UnmarshalMap(output.Item, &item); err != nil {
		return nil, err
	}
	return item, nil
}

func (c *Connector) baseStableItem(ref StreamRef, key map[string]types.AttributeValue, updatedAt int64) map[string]any {
	item := map[string]any{"createdAt": updatedAt, "updatedAt": updatedAt}
	for name, value := range key {
		var decoded any
		if err := attributevalue.Unmarshal(value, &decoded); err == nil {
			item[name] = decoded
		}
	}
	for name, value := range ref.ReplayAttributes {
		item[name] = value
	}
	return item
}

func (c *Connector) putItem(ctx context.Context, item map[string]any, condition string) error {
	av, err := marshalMap(cleanMap(item))
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{TableName: aws.String(c.options.TableName), Item: av}
	if condition != "" {
		input.ConditionExpression = aws.String(condition)
		input.ExpressionAttributeNames = map[string]string{"#pk": c.options.partitionKeyName()}
	}
	_, err = c.ddb.PutItem(ctx, input)
	return err
}

func (c *Connector) resolve(ctx context.Context, streamID string) (StreamRef, error) {
	if streamID == "" {
		return StreamRef{}, fmt.Errorf("streamId is required")
	}
	if c.options.Resolver != nil {
		return c.options.Resolver.ResolveStream(ctx, streamID)
	}
	return StreamRef{Channel: c.options.channelPrefix() + streamID, RedisStream: c.options.streamPrefix() + streamID, ReplayAttributes: map[string]any{"streamId": streamID}}, nil
}

func cleanMap(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range input {
		if value == nil || value == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func redisIntField(item map[string]any, key string) (int, bool) {
	switch value := item[key].(type) {
	case int:
		return value, true
	case int32:
		return int(value), true
	case int64:
		return int(value), true
	case float64:
		return int(value), true
	default:
		return 0, false
	}
}

func marshalMap(input map[string]any) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMapWithOptions(input, func(options *attributevalue.EncoderOptions) { options.TagKey = "dynamodbav" })
}
