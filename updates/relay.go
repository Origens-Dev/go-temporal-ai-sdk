package updates

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
)

const (
	defaultSnapshotEveryChunks = 16
	defaultSnapshotEveryChars  = 1024
)

type FailurePolicy string

const (
	FailurePolicyStrict     FailurePolicy = "strict"
	FailurePolicyBestEffort FailurePolicy = "best-effort"
)

type Options struct {
	Visible                 bool          `json:"visible,omitempty"`
	StreamID                string        `json:"streamId,omitempty"`
	TargetRecordID          string        `json:"targetRecordId,omitempty"`
	Lane                    Lane          `json:"lane,omitempty"`
	AttemptID               string        `json:"attemptId,omitempty"`
	SnapshotEveryChunks     int           `json:"snapshotEveryChunks,omitempty"`
	SnapshotEveryCharacters int           `json:"snapshotEveryChars,omitempty"`
	FailurePolicy           FailurePolicy `json:"failurePolicy,omitempty"`
	Scope
}

type Clock func() time.Time

type Relay struct {
	connector          Connector
	options            Options
	attempts           map[string]*previewState
	snapshotEveryChunk int
	snapshotEveryChar  int
	now                Clock
	disabled           bool
}

type previewState struct {
	ref                    PreviewRef
	text                   strings.Builder
	object                 any
	elements               []any
	lastSnapshotSequence   int
	lastSnapshotTextLength int
	ended                  bool
	outcome                PreviewOutcome
}

func NewRelay(connector Connector, options Options) *Relay {
	return NewRelayWithClock(connector, options, time.Now)
}

func NewRelayWithClock(connector Connector, options Options, clock Clock) *Relay {
	if connector == nil {
		connector = NoopConnector{}
	}
	if options.Lane == "" {
		options.Lane = LaneText
	}
	if options.SnapshotEveryChunks <= 0 {
		options.SnapshotEveryChunks = defaultSnapshotEveryChunks
	}
	if options.SnapshotEveryCharacters <= 0 {
		options.SnapshotEveryCharacters = defaultSnapshotEveryChars
	}
	if clock == nil {
		clock = time.Now
	}
	return &Relay{
		connector:          connector,
		options:            options,
		attempts:           map[string]*previewState{},
		snapshotEveryChunk: options.SnapshotEveryChunks,
		snapshotEveryChar:  options.SnapshotEveryCharacters,
		now:                clock,
	}
}

func (r *Relay) Accept(ctx context.Context, part ai.StreamPart) error {
	if !r.enabled() {
		return nil
	}
	lane, meta, ok := classifyPart(r.options.Lane, part)
	if !ok {
		return nil
	}
	meta.scope = mergeScope(r.options.Scope, meta.scope)
	state, err := r.ensurePreview(ctx, lane, meta)
	if err != nil || state == nil {
		return err
	}
	if state.ended {
		return nil
	}
	if !meta.emit {
		return nil
	}
	state.ref.Sequence++
	if meta.delta != "" {
		state.text.WriteString(meta.delta)
	}
	if meta.hasObject {
		state.object = meta.object
	}
	if meta.element != nil {
		state.elements = append(state.elements, meta.element)
	}
	event := PreviewChunkEvent{
		BaseEvent:  r.baseEvent(EventTypePreviewChunk, previewEventID(state.ref.AttemptID, state.ref.Sequence, "chunk")),
		PreviewRef: state.ref,
		Chunk:      chunkFromPart(part, meta),
	}
	if err := r.connector.PublishUpdate(ctx, event); err != nil {
		return r.handleError(err)
	}
	if meta.hasObject || meta.element != nil || r.snapshotDue(state) {
		return r.checkpoint(ctx, state)
	}
	return nil
}

func (r *Relay) Succeed(ctx context.Context) error {
	return r.complete(ctx, PreviewOutcomeSucceeded, "")
}
func (r *Relay) Fail(ctx context.Context, reason string) error {
	return r.complete(ctx, PreviewOutcomeFailed, reason)
}
func (r *Relay) Cancel(ctx context.Context, reason string) error {
	return r.complete(ctx, PreviewOutcomeCanceled, reason)
}

func (r *Relay) complete(ctx context.Context, outcome PreviewOutcome, reason string) error {
	if !r.enabled() {
		return nil
	}
	keys := make([]string, 0, len(r.attempts))
	for key := range r.attempts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		state := r.attempts[key]
		if state.ended {
			continue
		}
		if err := r.checkpoint(ctx, state); err != nil {
			return err
		}
		state.ref.Sequence++
		snapshot := state.snapshot()
		event := PreviewEndEvent{
			BaseEvent:  r.baseEvent(EventTypePreviewEnd, previewEventID(state.ref.AttemptID, state.ref.Sequence, "end")),
			PreviewRef: state.ref,
			Outcome:    outcome,
			Reason:     reason,
			Snapshot:   &snapshot,
		}
		if err := r.connector.EndPreview(ctx, event); err != nil {
			return r.handleError(err)
		}
		state.ended = true
		state.outcome = outcome
	}
	return nil
}

func (r *Relay) Receipts() []PreviewReceipt {
	if r == nil {
		return nil
	}
	receipts := make([]PreviewReceipt, 0, len(r.attempts))
	keys := make([]string, 0, len(r.attempts))
	for key := range r.attempts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		state := r.attempts[key]
		if !state.ended {
			continue
		}
		receipts = append(receipts, PreviewReceipt{
			AttemptID:      state.ref.AttemptID,
			TargetRecordID: state.ref.TargetRecordID,
			Lane:           state.ref.Lane,
			Sequence:       state.ref.Sequence,
			Outcome:        state.outcome,
			Snapshot:       state.snapshot(),
			Scope:          state.ref.Scope,
		})
	}
	return receipts
}

func (r *Relay) ensurePreview(ctx context.Context, lane Lane, meta partMeta) (*previewState, error) {
	key := string(lane)
	if meta.scope.StepID != "" {
		key += ":step:" + meta.scope.StepID
	}
	if meta.toolCallID != "" {
		key += ":tool:" + meta.toolCallID
	}
	if state := r.attempts[key]; state != nil {
		return state, nil
	}
	baseAttempt := r.options.AttemptID
	if baseAttempt == "" {
		baseAttempt = "attempt"
	}
	attemptID := baseAttempt + ":" + string(lane)
	if meta.toolCallID != "" {
		attemptID += ":" + sanitize(meta.toolCallID)
	}
	targetRecordID := r.options.TargetRecordID
	if meta.toolCallID != "" {
		targetRecordID = "tool:" + meta.toolCallID
	} else if targetRecordID != "" && lane == LaneReasoning {
		targetRecordID += ":reasoning"
	}
	if targetRecordID == "" {
		targetRecordID = "message:" + r.options.StreamID
	}
	ref := PreviewRef{AttemptID: attemptID, TargetRecordID: targetRecordID, Lane: lane, Sequence: 0, Scope: meta.scope}
	event := PreviewBeginEvent{BaseEvent: r.baseEvent(EventTypePreviewBegin, previewEventID(attemptID, 0, "begin")), PreviewRef: ref}
	if err := r.connector.BeginPreview(ctx, event); err != nil {
		return nil, r.handleError(err)
	}
	if r.disabled {
		return nil, nil
	}
	state := &previewState{ref: ref}
	r.attempts[key] = state
	return state, nil
}

func (r *Relay) checkpoint(ctx context.Context, state *previewState) error {
	if state == nil || state.ended || r.disabled {
		return nil
	}
	if state.ref.Sequence == state.lastSnapshotSequence && state.lastSnapshotTextLength == state.text.Len() {
		return nil
	}
	event := PreviewSnapshotEvent{
		BaseEvent:  r.baseEvent(EventTypePreviewSnapshot, previewEventID(state.ref.AttemptID, state.ref.Sequence, "snapshot")),
		PreviewRef: state.ref,
		Snapshot:   state.snapshot(),
	}
	if err := r.connector.CheckpointPreview(ctx, event); err != nil {
		return r.handleError(err)
	}
	state.lastSnapshotSequence = state.ref.Sequence
	state.lastSnapshotTextLength = state.text.Len()
	return nil
}

func (r *Relay) snapshotDue(state *previewState) bool {
	return state.ref.Sequence-state.lastSnapshotSequence >= r.snapshotEveryChunk || state.text.Len()-state.lastSnapshotTextLength >= r.snapshotEveryChar
}

func (r *Relay) baseEvent(kind EventType, id string) BaseEvent {
	return BaseEvent{ProtocolVersion: ProtocolVersion, Type: kind, EventID: id, StreamID: r.options.StreamID, AgentID: r.options.AgentID, OccurredAt: r.now().UnixMilli()}
}

func (r *Relay) enabled() bool {
	return r != nil && r.options.Visible && r.options.StreamID != "" && !r.disabled
}

func (r *Relay) handleError(err error) error {
	if err == nil {
		return nil
	}
	if r.options.FailurePolicy == FailurePolicyBestEffort && errors.Is(err, ErrStreamNotFound) {
		r.disabled = true
		return nil
	}
	return err
}

func (s *previewState) snapshot() Snapshot {
	result := Snapshot{Text: s.text.String(), Object: s.object}
	if len(s.elements) > 0 {
		result.Elements = append([]any(nil), s.elements...)
	}
	return result
}

type partMeta struct {
	toolCallID string
	delta      string
	input      any
	element    any
	object     any
	hasObject  bool
	scope      Scope
	emit       bool
}

func classifyPart(defaultLane Lane, part ai.StreamPart) (Lane, partMeta, bool) {
	textLane := defaultLane
	if textLane != LaneObject {
		textLane = LaneText
	}
	scope := scopeFromPart(part)
	switch part.Type {
	case "stream-start":
		return textLane, partMeta{scope: scope}, true
	case "start-step", "response-metadata", "finish-step", "finish", "abort", "file", "reasoning-file", "source":
		return textLane, partMeta{scope: scope, emit: true}, true
	case "text-delta":
		if defaultLane == LaneObject && part.PartialOutput == nil {
			return "", partMeta{}, false
		}
		meta := partMeta{delta: part.TextDelta, scope: scope, emit: true}
		if defaultLane == LaneObject {
			meta.delta = ""
		}
		if part.PartialOutput != nil {
			meta.object, meta.hasObject = part.PartialOutput, true
		}
		return textLane, meta, true
	case "reasoning-delta":
		return LaneReasoning, partMeta{delta: part.ReasoningDelta, scope: scope, emit: true}, true
	case "tool-input-delta":
		return LaneToolInput, partMeta{toolCallID: part.ToolCallID, delta: part.ToolInputDelta, scope: scope, emit: true}, true
	case "tool-input-end":
		return LaneToolInput, partMeta{toolCallID: part.ToolCallID, delta: part.ToolInput, scope: scope, emit: true}, true
	case "tool-call":
		return LaneToolInput, partMeta{toolCallID: part.ToolCallID, input: part.ToolInput, scope: scope, emit: true}, true
	case "element":
		return LaneObject, partMeta{element: part.Element, scope: scope, emit: true}, true
	default:
		return "", partMeta{}, false
	}
}

func chunkFromPart(part ai.StreamPart, meta partMeta) map[string]any {
	chunk := map[string]any{"type": part.Type}
	if meta.delta != "" {
		chunk["delta"] = meta.delta
	}
	if meta.input != nil {
		chunk["input"] = meta.input
	}
	if meta.element != nil {
		chunk["element"] = meta.element
	}
	if meta.hasObject {
		chunk["object"] = meta.object
	}
	if part.ToolCallID != "" {
		chunk["toolCallId"] = part.ToolCallID
	}
	if part.ToolName != "" {
		chunk["toolName"] = part.ToolName
	}
	if part.FinishReason.Unified != "" {
		chunk["finishReason"] = part.FinishReason
	}
	if part.Content != nil {
		var value any
		if data, err := json.Marshal(part.Content); err == nil && json.Unmarshal(data, &value) == nil {
			chunk["content"] = value
		}
	}
	return chunk
}

func scopeFromPart(part ai.StreamPart) Scope {
	scope := Scope{StepID: part.StepID, StepType: part.StepType}
	if part.StepID != "" || part.StepType != "" || part.StepNumber != 0 {
		number := part.StepNumber
		scope.StepNumber = &number
	}
	return scope
}

func mergeScope(base Scope, override Scope) Scope {
	out := base
	if override.DisplayMode != "" {
		out.DisplayMode = override.DisplayMode
	}
	if override.AgentID != "" {
		out.AgentID = override.AgentID
	}
	if override.TaskID != "" {
		out.TaskID = override.TaskID
	}
	if override.TaskTitle != "" {
		out.TaskTitle = override.TaskTitle
	}
	if override.SkillName != "" {
		out.SkillName = override.SkillName
	}
	if override.StepID != "" {
		out.StepID = override.StepID
	}
	if override.StepNumber != nil {
		out.StepNumber = override.StepNumber
	}
	if override.StepType != "" {
		out.StepType = override.StepType
	}
	return out
}

func previewEventID(attemptID string, sequence int, suffix string) string {
	return fmt.Sprintf("preview:%s:%d:%s", attemptID, sequence, suffix)
}

func sanitize(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func StreamFromParts(parts []ai.StreamPart) <-chan ai.StreamPart {
	out := make(chan ai.StreamPart, len(parts))
	for _, part := range parts {
		out <- part
	}
	close(out)
	return out
}
