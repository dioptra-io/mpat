package main

import (
	"github.com/spf13/cobra"
)

func computeCmd() *cobra.Command {
	computeCmd := &cobra.Command{
		Use:   "compute",
		Short: "Compute derived tables from source data",
	}
	computeCmd.AddCommand(computeResultsFiesCmd())
	return computeCmd
}
