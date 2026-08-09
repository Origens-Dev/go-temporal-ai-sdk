package activities

import (
	"encoding/json"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
)

const ProviderOptionsKey = "temporal"

func extractStreamOptions(options ai.LanguageModelCallOptions) (ai.LanguageModelCallOptions, updates.Options) {
	providerOptions := cloneProviderOptions(options.ProviderOptions)
	streamOptions := parseStreamOptions(providerOptions[ProviderOptionsKey])
	delete(providerOptions, ProviderOptionsKey)
	options.ProviderOptions = providerOptions
	return options, streamOptions
}

func extractGenerateObjectStreamOptions(options ai.GenerateObjectOptions) (ai.GenerateObjectOptions, updates.Options) {
	providerOptions := cloneProviderOptions(options.ProviderOptions)
	streamOptions := parseStreamOptions(providerOptions[ProviderOptionsKey])
	delete(providerOptions, ProviderOptionsKey)
	options.ProviderOptions = providerOptions
	return options, streamOptions
}

func extractStreamObjectStreamOptions(options ai.StreamObjectOptions) (ai.StreamObjectOptions, updates.Options) {
	generateOptions, streamOptions := extractGenerateObjectStreamOptions(options.GenerateObjectOptions)
	options.GenerateObjectOptions = generateOptions
	return options, streamOptions
}

func parseStreamOptions(value any) updates.Options {
	if value == nil {
		return updates.Options{}
	}
	if opts, ok := value.(updates.Options); ok {
		return opts
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return updates.Options{}
	}
	var opts updates.Options
	if err := json.Unmarshal(bytes, &opts); err != nil {
		return updates.Options{}
	}
	return opts
}

func cloneProviderOptions(options ai.ProviderOptions) ai.ProviderOptions {
	if len(options) == 0 {
		return nil
	}
	out := ai.ProviderOptions{}
	for key, value := range options {
		out[key] = value
	}
	return out
}
