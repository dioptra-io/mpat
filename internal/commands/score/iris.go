package score

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	adapterv1 "dioptra-io/ufuk-research/pkg/adapter/v1"
	clientv1 "dioptra-io/ufuk-research/pkg/client/v1"
	"dioptra-io/ufuk-research/pkg/util"
)

func retrieveIrisTablestoProcess(args []string) ([]string, []string, error) {
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

var ScoreIrisCmd = &cobra.Command{
	Use:   "iris <table-names...>",
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

		irisCHClient, err := clientv1.NewClickHouseClient(viper.GetString("iris-research-clickhouse-dsn"))
		if err != nil {
			panic(err)
		}

		err = adapterv1.ComputeRouteScoresTable(irisCHClient, resultTableNames, routesTableNames, fChunkSize, fNumWorkers, fForceTableReset)
		if err != nil {
			panic(err)
		}

		err = adapterv1.WriteToFile(output, irisCHClient)
		if err != nil {
			panic(err)
		}

		logger.Infoln("Done!")
	},
}
