package temporalai

import (
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"go.temporal.io/sdk/workflow"
)

type ActivityOptions struct {
	Default                workflow.ActivityOptions
	LanguageModel          workflow.ActivityOptions
	EmbeddingModel         workflow.ActivityOptions
	Tool                   workflow.ActivityOptions
	LocalLanguageModel     workflow.LocalActivityOptions
	LocalEmbeddingModel    workflow.LocalActivityOptions
	LocalTool              workflow.LocalActivityOptions
	Record                 workflow.ActivityOptions
	LanguageModelBoundary  activities.ToolExecutionBoundary
	EmbeddingModelBoundary activities.ToolExecutionBoundary
	AgentID                string
	CompiledRevision       string
}

func defaultActivityOptions(summary string) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		Summary:             summary,
	}
}

func languageModelActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	return mergeActivityOptions(defaultActivityOptions(activities.InvokeModelActivity), mergeActivityOptions(options.Default, options.LanguageModel))
}

func generateObjectActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	return mergeActivityOptions(defaultActivityOptions(activities.GenerateObjectActivity), mergeActivityOptions(options.Default, options.LanguageModel))
}

func streamObjectActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	return mergeActivityOptions(defaultActivityOptions(activities.StreamObjectActivity), mergeActivityOptions(options.Default, options.LanguageModel))
}

func localStreamObjectActivityOptions(options ActivityOptions) workflow.LocalActivityOptions {
	return mergeLocalActivityOptions(defaultLocalLanguageModelActivityOptions(), options.LocalLanguageModel)
}

func embeddingModelActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	return mergeActivityOptions(defaultActivityOptions(activities.InvokeEmbeddingModelActivity), mergeActivityOptions(options.Default, options.EmbeddingModel))
}

func toolActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	return mergeActivityOptions(defaultActivityOptions(activities.InvokeToolActivity), mergeActivityOptions(options.Default, options.Tool))
}

func streamModelActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	return mergeActivityOptions(defaultActivityOptions(activities.InvokeModelStreamActivity), mergeActivityOptions(options.Default, options.LanguageModel))
}

func localStreamModelActivityOptions(options ActivityOptions) workflow.LocalActivityOptions {
	return mergeLocalActivityOptions(defaultLocalLanguageModelActivityOptions(), options.LocalLanguageModel)
}

func defaultLocalLanguageModelActivityOptions() workflow.LocalActivityOptions {
	return workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Minute, Summary: activities.InvokeModelActivity}
}

func localLanguageModelActivityOptions(options ActivityOptions) workflow.LocalActivityOptions {
	return mergeLocalActivityOptions(defaultLocalLanguageModelActivityOptions(), options.LocalLanguageModel)
}

func defaultLocalGenerateObjectActivityOptions() workflow.LocalActivityOptions {
	return workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Minute, Summary: activities.GenerateObjectActivity}
}

func localGenerateObjectActivityOptions(options ActivityOptions) workflow.LocalActivityOptions {
	return mergeLocalActivityOptions(defaultLocalGenerateObjectActivityOptions(), options.LocalLanguageModel)
}

func defaultLocalEmbeddingModelActivityOptions() workflow.LocalActivityOptions {
	return workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Minute, Summary: activities.InvokeEmbeddingModelActivity}
}

func localEmbeddingModelActivityOptions(options ActivityOptions) workflow.LocalActivityOptions {
	return mergeLocalActivityOptions(defaultLocalEmbeddingModelActivityOptions(), options.LocalEmbeddingModel)
}

func defaultLocalToolActivityOptions() workflow.LocalActivityOptions {
	return workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second, Summary: activities.InvokeToolActivity}
}

func localToolActivityOptions(options ActivityOptions) workflow.LocalActivityOptions {
	return mergeLocalActivityOptions(defaultLocalToolActivityOptions(), options.LocalTool)
}

func recordActivityOptions(options ActivityOptions) workflow.ActivityOptions {
	base := workflow.ActivityOptions{StartToCloseTimeout: time.Minute, Summary: activities.WriteRecordActivity}
	return mergeActivityOptions(base, mergeActivityOptions(options.Default, options.Record))
}

func InvokeModel(ctx workflow.Context, modelID string, options ai.LanguageModelCallOptions, activityOptions ...ActivityOptions) (*ai.LanguageModelGenerateResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	var wireResult activities.InvokeModelResult
	args := activities.InvokeModelArgs{
		AgentID:          ao.AgentID,
		CompiledRevision: ao.CompiledRevision,
		ModelID:          modelID,
		Options:          activities.LanguageModelCallOptionsFromAI(options),
	}
	var err error
	if ao.LanguageModelBoundary == activities.ToolExecutionBoundaryLocalActivity {
		ctx = workflow.WithLocalActivityOptions(ctx, localLanguageModelActivityOptions(ao))
		err = workflow.ExecuteLocalActivity(ctx, activities.InvokeModelActivity, args).Get(ctx, &wireResult)
	} else {
		ctx = workflow.WithActivityOptions(ctx, languageModelActivityOptions(ao))
		err = workflow.ExecuteActivity(ctx, activities.InvokeModelActivity, args).Get(ctx, &wireResult)
	}
	if err != nil {
		return nil, err
	}
	result := wireResult.ToAI()
	return &result, nil
}

func GenerateObject(ctx workflow.Context, modelID string, options ai.GenerateObjectOptions, activityOptions ...ActivityOptions) (*ai.GenerateObjectResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	var wireResult activities.GenerateObjectResult
	args := activities.GenerateObjectArgs{
		AgentID:          ao.AgentID,
		CompiledRevision: ao.CompiledRevision,
		ModelID:          modelID,
		Options:          activities.GenerateObjectOptionsFromAI(options),
	}
	var err error
	if ao.LanguageModelBoundary == activities.ToolExecutionBoundaryLocalActivity {
		ctx = workflow.WithLocalActivityOptions(ctx, localGenerateObjectActivityOptions(ao))
		err = workflow.ExecuteLocalActivity(ctx, activities.GenerateObjectActivity, args).Get(ctx, &wireResult)
	} else {
		ctx = workflow.WithActivityOptions(ctx, generateObjectActivityOptions(ao))
		err = workflow.ExecuteActivity(ctx, activities.GenerateObjectActivity, args).Get(ctx, &wireResult)
	}
	if err != nil {
		return nil, err
	}
	result := wireResult.ToAI()
	return &result, nil
}

func StreamObject(ctx workflow.Context, modelID string, options ai.StreamObjectOptions, activityOptions ...ActivityOptions) (*activities.StreamObjectAIResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	var wireResult activities.StreamObjectResult
	args := activities.StreamObjectArgs{
		AgentID:          ao.AgentID,
		CompiledRevision: ao.CompiledRevision,
		ModelID:          modelID,
		Options:          activities.StreamObjectOptionsFromAI(options),
	}
	var err error
	if ao.LanguageModelBoundary == activities.ToolExecutionBoundaryLocalActivity {
		ctx = workflow.WithLocalActivityOptions(ctx, localStreamObjectActivityOptions(ao))
		err = workflow.ExecuteLocalActivity(ctx, activities.StreamObjectActivity, args).Get(ctx, &wireResult)
	} else {
		ctx = workflow.WithActivityOptions(ctx, streamObjectActivityOptions(ao))
		err = workflow.ExecuteActivity(ctx, activities.StreamObjectActivity, args).Get(ctx, &wireResult)
	}
	if err != nil {
		return nil, err
	}
	result := wireResult.ToAI()
	return &result, nil
}

func InvokeModelStream(ctx workflow.Context, modelID string, options ai.LanguageModelCallOptions, activityOptions ...ActivityOptions) (*activities.InvokeModelStreamAIResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	var wireResult activities.InvokeModelStreamResult
	args := activities.InvokeModelStreamArgs{
		AgentID:          ao.AgentID,
		CompiledRevision: ao.CompiledRevision,
		ModelID:          modelID,
		Options:          activities.LanguageModelCallOptionsFromAI(options),
	}
	var err error
	if ao.LanguageModelBoundary == activities.ToolExecutionBoundaryLocalActivity {
		ctx = workflow.WithLocalActivityOptions(ctx, localStreamModelActivityOptions(ao))
		err = workflow.ExecuteLocalActivity(ctx, activities.InvokeModelStreamActivity, args).Get(ctx, &wireResult)
	} else {
		ctx = workflow.WithActivityOptions(ctx, streamModelActivityOptions(ao))
		err = workflow.ExecuteActivity(ctx, activities.InvokeModelStreamActivity, args).Get(ctx, &wireResult)
	}
	if err != nil {
		return nil, err
	}
	result := wireResult.ToAI()
	return &result, nil
}

func InvokeEmbeddingModel(ctx workflow.Context, modelID string, options ai.EmbeddingModelCallOptions, activityOptions ...ActivityOptions) (*ai.EmbeddingModelResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	var result ai.EmbeddingModelResult
	args := activities.InvokeEmbeddingModelArgs{
		AgentID:          ao.AgentID,
		CompiledRevision: ao.CompiledRevision,
		ModelID:          modelID,
		Values:           options.Values,
		ProviderOptions:  options.ProviderOptions,
		Headers:          options.Headers,
	}
	var err error
	if ao.EmbeddingModelBoundary == activities.ToolExecutionBoundaryLocalActivity {
		ctx = workflow.WithLocalActivityOptions(ctx, localEmbeddingModelActivityOptions(ao))
		err = workflow.ExecuteLocalActivity(ctx, activities.InvokeEmbeddingModelActivity, args).Get(ctx, &result)
	} else {
		ctx = workflow.WithActivityOptions(ctx, embeddingModelActivityOptions(ao))
		err = workflow.ExecuteActivity(ctx, activities.InvokeEmbeddingModelActivity, args).Get(ctx, &result)
	}
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func InvokeTool(ctx workflow.Context, args activities.InvokeToolArgs, activityOptions ...ActivityOptions) (*activities.InvokeToolResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	if args.AgentID == "" {
		args.AgentID = ao.AgentID
	}
	if args.CompiledRevision == "" {
		args.CompiledRevision = ao.CompiledRevision
	}
	ctx = workflow.WithActivityOptions(ctx, toolActivityOptions(ao))
	var result activities.InvokeToolResult
	err := workflow.ExecuteActivity(ctx, activities.InvokeToolActivity, args).Get(ctx, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func InvokeToolLocal(ctx workflow.Context, args activities.InvokeToolArgs, activityOptions ...ActivityOptions) (*activities.InvokeToolResult, error) {
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	if args.AgentID == "" {
		args.AgentID = ao.AgentID
	}
	if args.CompiledRevision == "" {
		args.CompiledRevision = ao.CompiledRevision
	}
	ctx = workflow.WithLocalActivityOptions(ctx, localToolActivityOptions(ao))
	var result activities.InvokeToolResult
	err := workflow.ExecuteLocalActivity(ctx, activities.InvokeToolActivity, args).Get(ctx, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// WriteRecord durably accepts a complete canonical record. It always runs in a
// separate activity from model and tool execution so persistence retries cannot
// repeat those side effects.
func WriteRecord(ctx workflow.Context, streamID string, record updates.WorkflowRecord, acceptedAttemptID string, activityOptions ...ActivityOptions) error {
	if streamID == "" {
		return nil
	}
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	now := workflow.Now(ctx).UnixMilli()
	if record.UpdatedAt == 0 {
		record.UpdatedAt = now
	}
	event := updates.NewRecordUpsertEvent(streamID, record, acceptedAttemptID, now)
	ctx = workflow.WithActivityOptions(ctx, recordActivityOptions(ao))
	return workflow.ExecuteActivity(ctx, activities.WriteRecordActivity, activities.WriteRecordArgs{Event: event}).Get(ctx, nil)
}

// EndStream persists the terminal subscription event. Callers must invoke it
// only after all accepted records have been written.
func EndStream(ctx workflow.Context, streamID string, outcome updates.StreamOutcome, errorText string, activityOptions ...ActivityOptions) error {
	if streamID == "" {
		return nil
	}
	ao := ActivityOptions{}
	if len(activityOptions) > 0 {
		ao = activityOptions[0]
	}
	event := updates.NewStreamEndEvent(streamID, outcome, errorText, workflow.Now(ctx).UnixMilli())
	ctx = workflow.WithActivityOptions(ctx, recordActivityOptions(ao))
	return workflow.ExecuteActivity(ctx, activities.EndStreamActivity, activities.EndStreamArgs{Event: event}).Get(ctx, nil)
}

func mergeActivityOptions(base, override workflow.ActivityOptions) workflow.ActivityOptions {
	out := base
	if override.TaskQueue != "" {
		out.TaskQueue = override.TaskQueue
	}
	if override.ScheduleToCloseTimeout != 0 {
		out.ScheduleToCloseTimeout = override.ScheduleToCloseTimeout
	}
	if override.ScheduleToStartTimeout != 0 {
		out.ScheduleToStartTimeout = override.ScheduleToStartTimeout
	}
	if override.StartToCloseTimeout != 0 {
		out.StartToCloseTimeout = override.StartToCloseTimeout
	}
	if override.HeartbeatTimeout != 0 {
		out.HeartbeatTimeout = override.HeartbeatTimeout
	}
	if override.WaitForCancellation {
		out.WaitForCancellation = true
	}
	if override.ActivityID != "" {
		out.ActivityID = override.ActivityID
	}
	if override.RetryPolicy != nil {
		out.RetryPolicy = override.RetryPolicy
	}
	if override.Summary != "" {
		out.Summary = override.Summary
	}
	return out
}

func mergeLocalActivityOptions(base, override workflow.LocalActivityOptions) workflow.LocalActivityOptions {
	out := base
	if override.ScheduleToCloseTimeout != 0 {
		out.ScheduleToCloseTimeout = override.ScheduleToCloseTimeout
	}
	if override.StartToCloseTimeout != 0 {
		out.StartToCloseTimeout = override.StartToCloseTimeout
	}
	if override.RetryPolicy != nil {
		out.RetryPolicy = override.RetryPolicy
	}
	if override.Summary != "" {
		out.Summary = override.Summary
	}
	return out
}
