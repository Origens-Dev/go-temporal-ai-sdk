package activities

import (
	"context"
	"errors"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

func (a *Activities) consumeObjectStream(ctx context.Context, relay *updates.Relay, streamResult *ai.StreamObjectResult) (*StreamObjectResult, error) {
	stream := streamResult.Stream
	elementsStream := streamResult.Elements
	parts := []ai.ObjectStreamPart{}
	elements := []any{}
	finishRelayed := false

	for stream != nil || elementsStream != nil {
		select {
		case <-ctx.Done():
			reason := ctx.Err().Error()
			_ = relay.Cancel(context.Background(), reason)
			return nil, ctx.Err()
		case element, ok := <-elementsStream:
			if !ok {
				elementsStream = nil
				continue
			}
			elements = append(elements, element)
		case part, ok := <-stream:
			if !ok {
				stream = nil
				continue
			}
			if part.Err != nil {
				_ = relay.Fail(ctx, part.Err.Error())
				return nil, part.Err
			}
			parts = append(parts, part)
			if part.Type == "finish" && finishRelayed {
				continue
			}
			if err := relayObjectStreamPart(ctx, relay, part); err != nil {
				_ = relay.Fail(ctx, err.Error())
				return nil, err
			}
			if part.Type == "finish" {
				finishRelayed = true
			}
		}
	}

	if err := relay.Succeed(ctx); err != nil {
		return nil, err
	}
	request := streamResult.Request
	response := ResponseMetadataFromAI(streamResult.Response)
	return &StreamObjectResult{
		StreamParts:     ObjectStreamPartsFromAI(parts),
		Elements:        elements,
		Request:         &request,
		Response:        &response,
		PreviewReceipts: relay.Receipts(),
	}, nil
}

func relayObjectStreamPart(ctx context.Context, relay *updates.Relay, part ai.ObjectStreamPart) error {
	switch part.Type {
	case "text-delta":
		return relay.Accept(ctx, ai.StreamPart{
			Type:      "text-delta",
			TextDelta: part.TextDelta,
			Raw:       part.Raw,
		})
	case "object":
		return relay.Accept(ctx, ai.StreamPart{
			Type:          "text-delta",
			PartialOutput: part.Object,
			Raw:           part.Raw,
		})
	case "element":
		return relay.Accept(ctx, ai.StreamPart{
			Type:    "element",
			Element: part.Element,
			Raw:     part.Raw,
		})
	case "finish":
		return relay.Accept(ctx, ai.StreamPart{
			Type:             "finish",
			FinishReason:     part.FinishReason,
			Usage:            part.Usage,
			Warnings:         part.Warnings,
			ProviderMetadata: part.ProviderMetadata,
			Raw:              part.Raw,
		})
	case "abort":
		return relay.Accept(ctx, ai.StreamPart{
			Type:        "abort",
			AbortReason: part.AbortReason,
			Raw:         part.Raw,
		})
	case "error":
		if part.Err != nil {
			return part.Err
		}
		if part.Raw != nil {
			return errors.New("object stream error")
		}
		return nil
	default:
		return nil
	}
}
