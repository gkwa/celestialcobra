package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "celestialcobra",
	Short: "Query DynamoDB with user-friendly interface",
	Long:  `CelestialCobra is a CLI tool to query DynamoDB tables with intuitive arguments including human-readable durations.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Global flags could be defined here
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.celestialcobra.yaml)")
	rootCmd.PersistentFlags().StringVar(&awsRegion, "region", "us-east-1", "AWS region")
}

var (
	cfgFile   string
	awsRegion string
)
