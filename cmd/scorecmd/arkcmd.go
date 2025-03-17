package scorecmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/util"
)

func retrieveArkTablestoProcess(args []string) ([]string, []string, error) {
	var resultTableNames []string
	var routesTableNames []string

	if len(fBefore) != 0 || len(fAfter) != 0 {
		panic("retrieval of the result tables are not supported yet.")
	} else {
		// Get the table names from arguments
		resultTableNames = args
	}

	for _, resultTableName := range resultTableNames {
		// Cheap way to find the route tables, find a better way.
		routesTableNames = append(routesTableNames, strings.Replace(resultTableName, "results", "routes", 1))
	}

	return resultTableNames, routesTableNames, nil
}

var ScoreArkCmd = &cobra.Command{
	Use:   "ark",
	Short: "This script is used to compute the route score for the given tables",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		logger := util.GetLogger()
		resultTableNames, routesTableNames, err := retrieveIrisTablestoProcess(args)
		if err != nil {
			panic(err)
		}

		output, err := getOutput()
		if err != nil {
			panic(err)
		}
		defer output.Close()

		irisClient := client.FromDSN(viper.GetString("iris-research-clickhouse-dsn"))

		err = client.ComputeRouteScoresTable(irisClient, resultTableNames, routesTableNames, fChunkSize, fNumWorkers, fForceTableReset)
		if err != nil {
			panic(err)
		}

		err = client.WriteToFile(output, irisClient)
		if err != nil {
			panic(err)
		}

		logger.Infoln("Done!")
	},
}
