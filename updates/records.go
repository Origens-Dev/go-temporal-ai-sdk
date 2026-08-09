package updates

import "fmt"

func NewRecordUpsertEvent(streamID string, record WorkflowRecord, acceptedAttemptID string, occurredAt int64) RecordUpsertEvent {
	return RecordUpsertEvent{
		BaseEvent: BaseEvent{
			ProtocolVersion: ProtocolVersion,
			Type:            EventTypeRecordUpsert,
			EventID:         fmt.Sprintf("%s:v%d", record.RecordID, record.RecordVersion),
			StreamID:        streamID,
			AgentID:         record.Scope.AgentID,
			OccurredAt:      occurredAt,
		},
		AcceptedAttemptID: acceptedAttemptID,
		Record:            record,
	}
}

func NewStreamEndEvent(streamID string, outcome StreamOutcome, errorText string, occurredAt int64) StreamEndEvent {
	return NewAgentStreamEndEvent(streamID, "", outcome, errorText, occurredAt)
}

// NewAgentStreamEndEvent preserves the producing agent identity on terminal
// events, whose payload otherwise has no record or preview scope.
func NewAgentStreamEndEvent(streamID, agentID string, outcome StreamOutcome, errorText string, occurredAt int64) StreamEndEvent {
	return StreamEndEvent{
		BaseEvent: BaseEvent{
			ProtocolVersion: ProtocolVersion,
			Type:            EventTypeStreamEnd,
			EventID:         fmt.Sprintf("stream:%s:end:%s", streamID, outcome),
			StreamID:        streamID,
			AgentID:         agentID,
			OccurredAt:      occurredAt,
		},
		Outcome: outcome,
		Error:   errorText,
	}
}
