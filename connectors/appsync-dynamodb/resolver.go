package appsyncdynamodb

import (
	"context"
	"fmt"

	"github.com/Origens-Dev/go-temporal-ai-sdk/updates"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoDBResolverOptions struct {
	DynamoDB         *dynamodb.Client
	TableName        string
	PartitionKeyName string
	Namespace        string

	OwnerField       string
	OwnerFallback    string
	ParentField      string
	ParentFallback   string
	ChannelFormatter func(namespace string, owner string, streamID string) string
}

type DynamoDBResolver struct {
	options DynamoDBResolverOptions
}

func NewDynamoDBResolver(options DynamoDBResolverOptions) *DynamoDBResolver {
	return &DynamoDBResolver{options: options}
}

func (r *DynamoDBResolver) ResolveStream(ctx context.Context, streamID string) (StreamRef, error) {
	if r == nil || r.options.DynamoDB == nil {
		return StreamRef{}, fmt.Errorf("dynamodb resolver requires a DynamoDB client")
	}
	partitionKey := r.options.PartitionKeyName
	if partitionKey == "" {
		partitionKey = defaultPartitionKeyName
	}
	out, err := r.options.DynamoDB.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.options.TableName),
		KeyConditionExpression: aws.String("#pk = :id"),
		ExpressionAttributeNames: map[string]string{
			"#pk": partitionKey,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{Value: streamID},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		return StreamRef{}, fmt.Errorf("resolving stream %q: %w", streamID, err)
	}
	if len(out.Items) == 0 {
		return StreamRef{}, updates.NewStreamNotFoundError(streamID)
	}
	var item map[string]any
	if err := attributevalue.UnmarshalMap(out.Items[0], &item); err != nil {
		return StreamRef{}, fmt.Errorf("unmarshalling stream %q: %w", streamID, err)
	}
	owner := stringField(item, firstString(r.options.OwnerField, "ownerUserId"))
	if owner == "" && r.options.OwnerFallback != "" {
		owner = stringField(item, r.options.OwnerFallback)
	} else if owner == "" {
		owner = stringField(item, "scopeId")
	}
	parent := stringField(item, firstString(r.options.ParentField, "parentConversationId"))
	if parent == "" && r.options.ParentFallback != "" {
		parent = stringField(item, r.options.ParentFallback)
	} else if parent == "" {
		parent = stringField(item, "conversationId")
	}
	namespace := firstString(r.options.Namespace, defaultNamespace)
	channel := ""
	if r.options.ChannelFormatter != nil {
		channel = r.options.ChannelFormatter(namespace, owner, streamID)
	} else if owner != "" {
		channel = "/" + trimSlashes(namespace) + "/" + owner + "/" + streamID
	}
	attrs := map[string]any{"streamId": streamID}
	if owner != "" {
		attrs["ownerUserId"] = owner
	}
	if parent != "" {
		attrs["parentConversationId"] = parent
	}
	return StreamRef{Channel: channel, ReplayAttributes: attrs}, nil
}

func stringField(item map[string]any, key string) string {
	if key == "" {
		return ""
	}
	if value, ok := item[key].(string); ok {
		return value
	}
	return ""
}

func firstString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
