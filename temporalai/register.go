package temporalai

import (
	"github.com/Origens-Dev/go-temporal-ai-sdk/activities"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func RegisterAgentWorkflow(w worker.Worker) {
	w.RegisterWorkflowWithOptions(AgentWorkflow, workflow.RegisterOptions{Name: AgentWorkflowName})
}

func RegisterActivities(w worker.Worker, acts *activities.Activities) {
	w.RegisterActivityWithOptions(acts.InvokeModel, activity.RegisterOptions{Name: activities.InvokeModelActivity})
	w.RegisterActivityWithOptions(acts.GenerateObject, activity.RegisterOptions{Name: activities.GenerateObjectActivity})
	w.RegisterActivityWithOptions(acts.StreamObject, activity.RegisterOptions{Name: activities.StreamObjectActivity})
	w.RegisterActivityWithOptions(acts.InvokeModelStream, activity.RegisterOptions{Name: activities.InvokeModelStreamActivity})
	w.RegisterActivityWithOptions(acts.InvokeEmbeddingModel, activity.RegisterOptions{Name: activities.InvokeEmbeddingModelActivity})
	w.RegisterActivityWithOptions(acts.InvokeTool, activity.RegisterOptions{Name: activities.InvokeToolActivity})
	w.RegisterActivityWithOptions(acts.WriteRecord, activity.RegisterOptions{Name: activities.WriteRecordActivity})
	w.RegisterActivityWithOptions(acts.EndStream, activity.RegisterOptions{Name: activities.EndStreamActivity})
}
