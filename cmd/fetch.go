package main

import (
	"github.com/spf13/cobra"
)

func fetchCmd() *cobra.Command {
	fetchCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch data from a source",
	}
	fetchCmd.AddCommand(fetchIrisResultsCmd())
	fetchCmd.AddCommand(fetchRipePrefixesCmd())
	return fetchCmd
}
