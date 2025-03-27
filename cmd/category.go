package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gkwa/celestialcobra/aws"
	"github.com/gkwa/celestialcobra/util"
	"github.com/spf13/cobra"
)

var (
	tableName string
	ageStr    string
	domain    string
)

func init() {
	categoryCmd := &cobra.Command{
		Use:   "category [category name]",
		Short: "Query DynamoDB by category",
		Long:  `Query DynamoDB by category with optional age parameter and domain filter.`,
		Args:  cobra.ExactArgs(1),
		RunE:  runCategoryCmd,
	}

	categoryCmd.Flags().StringVar(&tableName, "table", "dreamydungbeetle", "DynamoDB table name")
	categoryCmd.Flags().StringVar(&ageStr, "age", "7d", "Age for query, e.g., 1h, 2d, 3w")
	categoryCmd.Flags().StringVar(&domain, "domain", "", "Filter results by domain substring (e.g., trader will match www.traderjoes.com)")

	rootCmd.AddCommand(categoryCmd)
}

func runCategoryCmd(cmd *cobra.Command, args []string) error {
	category := args[0]

	// Parse age duration
	duration, err := util.ParseDuration(ageStr)
	if err != nil {
		return fmt.Errorf("invalid age format: %v", err)
	}

	// Get current date and past date based on duration
	now := time.Now().UTC()
	startDate := now.Add(-duration).Format("2006-01-02")
	endDate := now.Format("2006-01-02") + "~" // Adding ~ for DynamoDB range operator

	// Create AWS client and query
	client, err := aws.NewDynamoDBClient(context.Background(), awsRegion)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB client: %v", err)
	}

	response, err := client.QueryByCategory(context.Background(), tableName, category, startDate, endDate, domain)
	if err != nil {
		return fmt.Errorf("query failed: %v", err)
	}

	// Output the raw response to stdout
	if err := client.WriteRawResponse(os.Stdout, response); err != nil {
		return fmt.Errorf("failed to write response: %v", err)
	}

	return nil
}
