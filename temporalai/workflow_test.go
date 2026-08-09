package temporalai

import (
	"context"
	"testing"
	"time"

	"github.com/Origens-Dev/go-ai/packages/ai"
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestInvokeModelWorkflowHelper(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
			if args.AgentID != "agent-1" || args.CompiledRevision != "sha256:agent-1" || args.ModelID != "model-1" {
				t.Fatalf("args = %#v", args)
			}
			return &activities.InvokeModelResult{
				Content:      activities.PartsFromAI([]ai.Part{ai.TextPart{Text: "ok"}}),
				FinishReason: ai.FinishReason{Unified: ai.FinishStop},
			}, nil
		},
		activityRegisterOptions(activities.InvokeModelActivity),
	)

	env.ExecuteWorkflow(testInvokeModelWorkflow)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Fatalf("result = %q", result)
	}
}

func TestInvokeModelCanRunAsLocalActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var regularStarts int
	var localStarts int
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == activities.InvokeModelActivity {
			regularStarts++
		}
	})
	env.SetOnLocalActivityStartedListener(func(info *activity.Info, _ context.Context, _ []interface{}) {
		if info.ActivityType.Name == activities.InvokeModelActivity {
			localStarts++
		}
	})
	env.RegisterActivityWithOptions(
		func(_ context.Context, args activities.InvokeModelArgs) (*activities.InvokeModelResult, error) {
			if args.ModelID != "model-1" {
				t.Fatalf("model = %q", args.ModelID)
			}
			return &activities.InvokeModelResult{
				Content:      activities.PartsFromAI([]ai.Part{ai.TextPart{Text: "local ok"}}),
				FinishReason: ai.FinishReason{Unified: ai.FinishStop},
			}, nil
		},
		activityRegisterOptions(activities.InvokeModelActivity),
	)

	env.ExecuteWorkflow(testInvokeModelLocalWorkflow)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "local ok" {
		t.Fatalf("result = %q", result)
	}
	if localStarts != 1 {
		t.Fatalf("local model starts = %d, want 1", localStarts)
	}
	if regularStarts != 0 {
		t.Fatalf("regular model starts = %d, want 0", regularStarts)
	}
}

func TestGenerateObjectWorkflowHelper(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, args activities.GenerateObjectArgs) (*activities.GenerateObjectResult, error) {
			if args.AgentID != "agent-1" || args.CompiledRevision != "sha256:agent-1" || args.ModelID != "model-1" {
				t.Fatalf("args = %#v", args)
			}
			if args.Options.SchemaName != "profile" {
				t.Fatalf("schema name = %q", args.Options.SchemaName)
			}
			return &activities.GenerateObjectResult{
				Object:       map[string]any{"name": "Ada"},
				FinishReason: ai.FinishStop,
			}, nil
		},
		activityRegisterOptions(activities.GenerateObjectActivity),
	)

	env.ExecuteWorkflow(testGenerateObjectWorkflow)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "Ada" {
		t.Fatalf("result = %q", result)
	}
}

func TestGenerateObjectCanRunAsLocalActivity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var regularStarts int
	var localStarts int
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == activities.GenerateObjectActivity {
			regularStarts++
		}
	})
	env.SetOnLocalActivityStartedListener(func(info *activity.Info, _ context.Context, _ []interface{}) {
		if info.ActivityType.Name == activities.GenerateObjectActivity {
			localStarts++
		}
	})
	env.RegisterActivityWithOptions(
		func(_ context.Context, args activities.GenerateObjectArgs) (*activities.GenerateObjectResult, error) {
			if args.ModelID != "model-1" {
				t.Fatalf("model = %q", args.ModelID)
			}
			return &activities.GenerateObjectResult{
				Object:       map[string]any{"name": "Grace"},
				FinishReason: ai.FinishStop,
			}, nil
		},
		activityRegisterOptions(activities.GenerateObjectActivity),
	)

	env.ExecuteWorkflow(testGenerateObjectLocalWorkflow)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "Grace" {
		t.Fatalf("result = %q", result)
	}
	if localStarts != 1 {
		t.Fatalf("local object starts = %d, want 1", localStarts)
	}
	if regularStarts != 0 {
		t.Fatalf("regular object starts = %d, want 0", regularStarts)
	}
}

func TestStreamObjectWorkflowHelper(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, args activities.StreamObjectArgs) (*activities.StreamObjectResult, error) {
			if args.AgentID != "agent-1" || args.CompiledRevision != "sha256:agent-1" || args.ModelID != "model-1" {
				t.Fatalf("args = %#v", args)
			}
			if args.Options.Output != ai.OutputArray {
				t.Fatalf("output = %q", args.Options.Output)
			}
			return &activities.StreamObjectResult{
				StreamParts: []activities.ObjectStreamPart{
					{Type: "element", Element: map[string]any{"name": "Ada"}},
					{Type: "finish", FinishReason: ai.FinishReason{Unified: ai.FinishStop}},
				},
				Elements: []any{map[string]any{"name": "Ada"}},
			}, nil
		},
		activityRegisterOptions(activities.StreamObjectActivity),
	)

	env.ExecuteWorkflow(testStreamObjectWorkflow)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result != "Ada" {
		t.Fatalf("result = %q", result)
	}
}

func TestInvokeActivityOptionsDefaultSummaries(t *testing.T) {
	if got := languageModelActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeModelActivity {
		t.Fatalf("language model summary = %q", got)
	}
	if got := generateObjectActivityOptions(ActivityOptions{}).Summary; got != activities.GenerateObjectActivity {
		t.Fatalf("object summary = %q", got)
	}
	if got := streamObjectActivityOptions(ActivityOptions{}).Summary; got != activities.StreamObjectActivity {
		t.Fatalf("stream object summary = %q", got)
	}
	if got := streamModelActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeModelStreamActivity {
		t.Fatalf("stream model summary = %q", got)
	}
	if got := embeddingModelActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeEmbeddingModelActivity {
		t.Fatalf("embedding model summary = %q", got)
	}
	if got := toolActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeToolActivity {
		t.Fatalf("tool summary = %q", got)
	}
	if got := recordActivityOptions(ActivityOptions{}).Summary; got != activities.WriteRecordActivity {
		t.Fatalf("record summary = %q", got)
	}
	if got := localLanguageModelActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeModelActivity {
		t.Fatalf("local language model summary = %q", got)
	}
	if got := localGenerateObjectActivityOptions(ActivityOptions{}).Summary; got != activities.GenerateObjectActivity {
		t.Fatalf("local object summary = %q", got)
	}
	if got := localEmbeddingModelActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeEmbeddingModelActivity {
		t.Fatalf("local embedding model summary = %q", got)
	}
	if got := localToolActivityOptions(ActivityOptions{}).Summary; got != activities.InvokeToolActivity {
		t.Fatalf("local tool summary = %q", got)
	}
}

func TestRemoteActivityDefaultsHeartbeatButRecordDoesNot(t *testing.T) {
	if got := languageModelActivityOptions(ActivityOptions{}).HeartbeatTimeout; got != 30*time.Second {
		t.Fatalf("model heartbeat timeout = %s", got)
	}
	if got := toolActivityOptions(ActivityOptions{}).HeartbeatTimeout; got != 30*time.Second {
		t.Fatalf("tool heartbeat timeout = %s", got)
	}
	if got := recordActivityOptions(ActivityOptions{}).HeartbeatTimeout; got != 0 {
		t.Fatalf("record heartbeat timeout = %s", got)
	}
}

func TestWriteRecordAndEndStreamWorkflowHelpers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var records []activities.WriteRecordArgs
	var terminals []activities.EndStreamArgs
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.WriteRecordArgs) error {
		records = append(records, args)
		return nil
	}, activityRegisterOptions(activities.WriteRecordActivity))
	env.RegisterActivityWithOptions(func(_ context.Context, args activities.EndStreamArgs) error {
		terminals = append(terminals, args)
		return nil
	}, activityRegisterOptions(activities.EndStreamActivity))
	env.ExecuteWorkflow(testWriteRecordWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Event.AcceptedAttemptID != "attempt-1" || records[0].Event.Record.UpdatedAt == 0 {
		t.Fatalf("records = %#v", records)
	}
	if len(terminals) != 1 || terminals[0].Event.Outcome != updates.StreamOutcomeCompleted {
		t.Fatalf("terminals = %#v", terminals)
	}
}

func testInvokeModelWorkflow(ctx workflow.Context) (string, error) {
	result, err := InvokeModel(ctx, "model-1", ai.LanguageModelCallOptions{}, testRuntimeActivityOptions())
	if err != nil {
		return "", err
	}
	return ai.TextFromParts(result.Content), nil
}

func testWriteRecordWorkflow(ctx workflow.Context) error {
	record := updates.WorkflowRecord{RecordID: "message:1", RecordVersion: 1, Kind: updates.RecordKindMessage, Status: "completed", Data: map[string]any{"text": "ok"}}
	if err := WriteRecord(ctx, "stream-1", record, "attempt-1"); err != nil {
		return err
	}
	return EndStream(ctx, "stream-1", updates.StreamOutcomeCompleted, "")
}

func testGenerateObjectWorkflow(ctx workflow.Context) (string, error) {
	result, err := GenerateObject(ctx, "model-1", ai.GenerateObjectOptions{
		SchemaName: "profile",
		Schema:     map[string]any{"type": "object"},
	}, testRuntimeActivityOptions())
	if err != nil {
		return "", err
	}
	return result.Object.(map[string]any)["name"].(string), nil
}

func testGenerateObjectLocalWorkflow(ctx workflow.Context) (string, error) {
	result, err := GenerateObject(ctx, "model-1", ai.GenerateObjectOptions{}, ActivityOptions{
		LanguageModelBoundary: activities.ToolExecutionBoundaryLocalActivity,
		LocalLanguageModel: workflow.LocalActivityOptions{
			StartToCloseTimeout: time.Second,
		},
	})
	if err != nil {
		return "", err
	}
	return result.Object.(map[string]any)["name"].(string), nil
}

func testStreamObjectWorkflow(ctx workflow.Context) (string, error) {
	result, err := StreamObject(ctx, "model-1", ai.StreamObjectOptions{
		GenerateObjectOptions: ai.GenerateObjectOptions{
			Output: ai.OutputArray,
			Schema: map[string]any{"type": "object"},
		},
	}, testRuntimeActivityOptions())
	if err != nil {
		return "", err
	}
	element := result.Elements[0].(map[string]any)
	return element["name"].(string), nil
}

func testInvokeModelLocalWorkflow(ctx workflow.Context) (string, error) {
	result, err := InvokeModel(ctx, "model-1", ai.LanguageModelCallOptions{}, ActivityOptions{
		LanguageModelBoundary: activities.ToolExecutionBoundaryLocalActivity,
		LocalLanguageModel: workflow.LocalActivityOptions{
			StartToCloseTimeout: time.Second,
		},
	})
	if err != nil {
		return "", err
	}
	return ai.TextFromParts(result.Content), nil
}

func activityRegisterOptions(name string) activity.RegisterOptions {
	return activity.RegisterOptions{Name: name}
}

func testRuntimeActivityOptions() ActivityOptions {
	return ActivityOptions{AgentID: "agent-1", CompiledRevision: "sha256:agent-1"}
}
