package activities

import (
	"context"
	"encoding/json"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

func failRelay(ctx context.Context, relay *updates.Relay, err error) {
	if err == nil {
		return
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = relay.Cancel(context.Background(), ctxErr.Error())
		return
	}
	_ = relay.Fail(ctx, err.Error())
}

func relayGenerateResult(ctx context.Context, relay *updates.Relay, result *ai.LanguageModelGenerateResult) error {
	for _, content := range result.Content {
		for _, part := range finalContentStreamParts(content) {
			if err := relay.Accept(ctx, part); err != nil {
				return err
			}
		}
	}
	return relay.Accept(ctx, ai.StreamPart{
		Type:             "finish",
		FinishReason:     result.FinishReason,
		Usage:            result.Usage,
		Warnings:         result.Warnings,
		Request:          result.Request,
		Response:         result.Response,
		ProviderMetadata: result.ProviderMetadata,
	})
}

func relayGenerateObjectResult(ctx context.Context, relay *updates.Relay, result *ai.GenerateObjectResult) error {
	if result.Reasoning != "" {
		if err := relay.Accept(ctx, ai.StreamPart{Type: "reasoning-delta", ReasoningDelta: result.Reasoning}); err != nil {
			return err
		}
	}
	if result.Text != "" || result.Object != nil {
		if err := relay.Accept(ctx, ai.StreamPart{
			Type:          "text-delta",
			TextDelta:     result.Text,
			PartialOutput: result.Object,
		}); err != nil {
			return err
		}
	}
	return relay.Accept(ctx, ai.StreamPart{
		Type: "finish",
		FinishReason: ai.FinishReason{
			Unified: result.FinishReason,
			Raw:     result.RawFinishReason,
		},
		Usage:            result.Usage,
		Warnings:         result.Warnings,
		Request:          result.Request,
		Response:         result.Response,
		ProviderMetadata: result.ProviderMetadata,
	})
}

func finalContentStreamParts(content ai.Part) []ai.StreamPart {
	switch part := content.(type) {
	case ai.TextPart:
		if part.Text == "" {
			return nil
		}
		return []ai.StreamPart{{
			Type:             "text-delta",
			TextDelta:        part.Text,
			ProviderMetadata: part.ProviderMetadata,
		}}
	case ai.ReasoningPart:
		if part.Text == "" {
			return nil
		}
		return []ai.StreamPart{{
			Type:             "reasoning-delta",
			ReasoningDelta:   part.Text,
			ProviderMetadata: part.ProviderMetadata,
		}}
	case ai.ToolCallPart:
		return []ai.StreamPart{{
			Type:             "tool-call",
			ToolCallID:       part.ToolCallID,
			ToolName:         part.ToolName,
			ToolInput:        finalToolInputRaw(part.Input, part.InputRaw),
			ToolMetadata:     part.ToolMetadata,
			ProviderMetadata: part.ProviderMetadata,
		}}
	case ai.FilePart, ai.ReasoningFilePart:
		return []ai.StreamPart{{Type: "file", Content: content}}
	case ai.SourcePart:
		return []ai.StreamPart{{Type: "source", Content: content}}
	default:
		return nil
	}
}

func finalToolInputRaw(input any, inputRaw string) string {
	if inputRaw != "" || input == nil {
		return inputRaw
	}
	data, err := json.Marshal(input)
	if err != nil {
		return inputRaw
	}
	return string(data)
}
