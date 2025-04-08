package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBClient encapsulates DynamoDB operations
type DynamoDBClient struct {
	client *dynamodb.Client
}

// DynamoDBResponse represents the raw DynamoDB response format
type DynamoDBResponse struct {
	Items            []map[string]types.AttributeValue `json:"Items"`
	Count            int32                             `json:"Count"`
	ScannedCount     int32                             `json:"ScannedCount"`
	ConsumedCapacity *types.ConsumedCapacity           `json:"ConsumedCapacity"`
}

// NewDynamoDBClient creates a new DynamoDB client
func NewDynamoDBClient(ctx context.Context, region string) (*DynamoDBClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	return &DynamoDBClient{client: client}, nil
}

// QueryByCategory queries the DynamoDB table by category within a date range
func (d *DynamoDBClient) QueryByCategory(ctx context.Context, tableName, category, startDate, endDate, domainSubstring string) (*DynamoDBResponse, error) {
	// Create expression attribute names
	expressionAttributeNames := map[string]string{
		"#cat": "category",
		"#ts":  "timestamp",
	}

	// Create expression attribute values
	expressionAttributeValues := map[string]types.AttributeValue{
		":category_term": &types.AttributeValueMemberS{Value: category},
		":start_date":    &types.AttributeValueMemberS{Value: startDate},
		":end_date_max":  &types.AttributeValueMemberS{Value: endDate},
	}

	// Build the key condition expression
	keyConditionExpr := "#cat = :category_term AND #ts BETWEEN :start_date AND :end_date_max"

	// Prepare to collect all items
	var allItems []map[string]types.AttributeValue
	var lastEvaluatedKey map[string]types.AttributeValue
	var totalCount int32

	for {
		// Build the query input
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(tableName),
			KeyConditionExpression:    aws.String(keyConditionExpr),
			ExpressionAttributeNames:  expressionAttributeNames,
			ExpressionAttributeValues: expressionAttributeValues,
			ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
			ExclusiveStartKey:         lastEvaluatedKey,
		}

		// Execute the query
		result, err := d.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %v", err)
		}

		// Append items
		allItems = append(allItems, result.Items...)
		totalCount += result.Count

		// Check if we've retrieved all items
		lastEvaluatedKey = result.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			break
		}
	}

	// Initialize response
	response := &DynamoDBResponse{
		Items:        allItems,
		Count:        totalCount,
		ScannedCount: totalCount,
	}

	// If domain substring is specified, filter the results client-side
	if domainSubstring != "" && len(response.Items) > 0 {
		filteredItems := []map[string]types.AttributeValue{}

		for _, item := range response.Items {
			// Check if the item has a domain attribute
			if domainAttr, ok := item["domain"]; ok {
				if domainVal, ok := domainAttr.(*types.AttributeValueMemberS); ok {
					// If the domain contains the substring, include this item
					if strings.Contains(strings.ToLower(domainVal.Value), strings.ToLower(domainSubstring)) {
						filteredItems = append(filteredItems, item)
					}
				}
			}
		}

		// Replace the items with our filtered list
		response.Items = filteredItems
		response.Count = int32(len(filteredItems))
	}

	return response, nil
}

// WriteRawResponse writes the raw DynamoDB response to the provided writer
func (d *DynamoDBClient) WriteRawResponse(w io.Writer, response *DynamoDBResponse) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(response)
}
