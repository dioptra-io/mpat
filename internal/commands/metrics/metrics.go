package metrics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func MetricsCmd() *cobra.Command {
	metricsCmd := &cobra.Command{
		Use:   "metrics <rt table> <fd table>",
		Short: "Compute metrics.",
		Long:  "Compute the metrics and present them as a table",
		Run:   processCmd,
	}

	return metricsCmd
}

func processCmd(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Help()
		return
	}
	clickHouseDSNString := viper.GetString("dsn")
	traceTable := args[0]
	decisionTable := args[1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clickHouseClient, err := clientv3.NewNativeSQLClient(clickHouseDSNString)
	if err != nil {
		logger.Errorf("ClickHouse database healthcheck failed: %v.\n", err)
		return
	}

	logger.Println("Database health check positive.")

	logger.Printf("Running for table %s.\n", traceTable)

	metricQueryMapOfRT := map[string]string{
		"num_routetrace_records":   "SELECT count(*) FROM %s.%s",
		"num_discovered_addresses": "SELECT count(DISTINCT reply_src_addr) FROM %s.%s",
		"num_vps":                  "SELECT count(DISTINCT probe_src_addr) FROM %s.%s WHERE startsWith(toString(probe_src_addr), '::ffff:')",
	}

	metricQueryMapOfFD := map[string]string{
		"num_fdecision_records":            "SELECT count(*) FROM %s.%s",
		"num_unique_near_addresses":        "SELECT count(DISTINCT near_addr) FROM %s.%s",
		"num_unique_far_addresses":         "SELECT count(DISTINCT far_addr) FROM %s.%s",
		"num_unique_fdecisions_discovered": "SELECT count(DISTINCT near_addr, far_addr, probe_dst_prefix) FROM %s.%s",
	}

	valuesMapOfRT, err := runQueries(ctx, clickHouseClient, traceTable, metricQueryMapOfRT)
	if err != nil {
		logger.Errorf("An error occured while computing metrics: %v.\n", err)
		return
	}

	valuesMapOfFD, err := runQueries(ctx, clickHouseClient, decisionTable, metricQueryMapOfFD)
	if err != nil {
		logger.Errorf("An error occured while computing metrics: %v.\n", err)
		return
	}

	periodSeconds := uint64(24 * 60 * 60)
	numVPs := (valuesMapOfRT["num_vps"]).(uint64)
	probeRatePerVP := float64(valuesMapOfRT["num_routetrace_records"].(uint64)) / float64(numVPs*periodSeconds)

	valuesMapOfAll := util.MergeMaps(util.MergeMaps(valuesMapOfFD, valuesMapOfRT), map[string]any{
		"period_seconds":                 periodSeconds,
		"probing_rate_per_second_per_vp": probeRatePerVP,
	})

	valuesMap := map[string]any{
		"mpat_version":    viper.GetString("version"),
		"routetrace_name": traceTable,
		"fdecision_name":  decisionTable,
		"metrics":         valuesMapOfAll,
	}

	data, err := json.Marshal(valuesMap)
	if err != nil {
		logger.Errorf("An error occured while converting the metrics to json: %s.\n", err)
	}

	fmt.Printf("%v\n", string(data))

	logger.Println("Done!")
}

func runQuery(ctx context.Context, client *clientv3.NativeSQLClient, table string, q string) (uint64, error) {
	var result uint64
	qFormatted := fmt.Sprintf(q, client.Database, table)
	if err := client.QueryRow(ctx, qFormatted).Scan(&result); err != nil {
		return 0, err
	}
	return result, nil
}

func runQueries(ctx context.Context, client *clientv3.NativeSQLClient, table string, metricInfo map[string]string) (map[string]any, error) {
	metricValues := make(map[string]any, len(metricInfo))

	for metricName, metricQuery := range metricInfo {
		if num, err := runQuery(ctx, client, table, metricQuery); err != nil {
			return nil, err
		} else {
			metricValues[metricName] = num
		}
	}

	return metricValues, nil
}
