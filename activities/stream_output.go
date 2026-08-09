package activities

import (
	"encoding/json"
	"reflect"

	"github.com/Origens-Dev/go-ai/packages/ai"
)

type partialOutputTracker struct {
	enabled           bool
	text              string
	haveLast          bool
	last              any
	publishedElements int
}

func newPartialOutputTracker(format *ai.ResponseFormat) *partialOutputTracker {
	if format == nil || format.Type != "json" {
		return nil
	}
	return &partialOutputTracker{enabled: true}
}

func (t *partialOutputTracker) enrich(part ai.StreamPart) (ai.StreamPart, []ai.StreamPart) {
	if t == nil || !t.enabled || part.Type != "text-delta" {
		return part, nil
	}
	t.text += part.TextDelta
	value, err := ai.ParsePartialJSON(t.text)
	if err != nil || value == nil {
		return part, nil
	}
	if !t.haveLast || !reflect.DeepEqual(t.last, value) {
		part.PartialOutput = value
		t.haveLast = true
		t.last = value
	}
	elements, nextPublished := newPartialElements(t.text, value, t.publishedElements)
	t.publishedElements = nextPublished
	if len(elements) == 0 {
		return part, nil
	}
	extra := make([]ai.StreamPart, 0, len(elements))
	for _, element := range elements {
		extra = append(extra, ai.StreamPart{
			Type:       "element",
			ID:         part.ID,
			StepID:     part.StepID,
			StepNumber: part.StepNumber,
			StepType:   part.StepType,
			Element:    element,
		})
	}
	return part, extra
}

func newPartialElements(text string, value any, published int) ([]any, int) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, published
	}
	elements, ok := object["elements"].([]any)
	if !ok || published >= len(elements) {
		return nil, published
	}
	if !completeJSON(text) && len(elements) > 0 {
		elements = elements[:len(elements)-1]
	}
	if published >= len(elements) {
		return nil, published
	}
	return append([]any(nil), elements[published:]...), len(elements)
}

func completeJSON(text string) bool {
	var value any
	return json.Unmarshal([]byte(text), &value) == nil
}
