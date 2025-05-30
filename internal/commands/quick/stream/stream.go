package stream

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	apiv2 "github.com/dioptra-io/ufuk-research/api/v2"
	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

func StreamCmd() *cobra.Command {
	streamCmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream jsonl to stderr",
		Long:  "Streams a given type of object to stdout",
		Args:  streamCmdArgs,
		Run:   streamCmd,
	}

	irisResultsCmd := &cobra.Command{
		Use:   "iris-results",
		Short: "Stream iris resutls",
		Long:  "Streams the rows of results table from the Iris",
		Args:  irisResultsCmdArgs,
		Run:   irisResultsCmd,
	}

	arkResultsCmd := &cobra.Command{
		Use:   "ark-results",
		Short: "Stream ark resutls",
		Long:  "Streams the rows of results table from the Ark",
		Args:  arkResultsCmdArgs,
		Run:   arkResultsCmd,
	}

	streamCmd.AddCommand(irisResultsCmd)
	streamCmd.AddCommand(arkResultsCmd)
	return streamCmd
}

func streamCmdArgs(cmd *cobra.Command, args []string) error {
	return nil
}

func streamCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func irisResultsCmdArgs(cmd *cobra.Command, args []string) error {
	return nil
}

func irisResultsCmd(cmd *cobra.Command, args []string) {
	productionDSN := viper.GetString("production_dsn")

	sqlClient, err := clientv2.NewSQLClientWithHealthCheck(productionDSN)
	if err != nil {
		logger.Panicf("Cannot connect to the SQL client for ClickHouse research instance: %s\n", err)
		return
	}

	query := queries.SelectFromTables(sqlClient.Database(), args)
	rowCh, errCh := streamIrisResultsTableRow(*sqlClient, query)

	logger.Debugln("Started streaming from the results table.")

	for item := range rowCh {
		logger.Debugln("Received one object.")

		data, err := json.Marshal(item)
		if err != nil {
			logger.Panicf("Error occured while converting the object into json: %v\n", err)
			return
		}

		fmt.Println(string(data))
	}

	select {
	case err := <-errCh:
		logger.Panicf("Error occured while streaming the table: %v\n", err)
		return
	default:
	}

	logger.Println("Stream completed without any errors!")
}

func streamIrisResultsTableRow(sqlClient clientv2.SQLClient, query string) (<-chan apiv2.IrisResultsTableRow, <-chan error) {
	// setting this to a big number prevented the "connection reset by peer" error
	outCh := make(chan apiv2.IrisResultsTableRow, 10000)
	errCh := make(chan error, 1)

	go func() {
		defer close(outCh)

		stream, err := sqlClient.Download(query)
		if err != nil {
			errCh <- fmt.Errorf("download error: %v", err)
			return
		}
		defer stream.Close()

		stream2 := io.TeeReader(stream, os.Stdout)
		reader := csv.NewReader(stream2)
		// no need to skip header

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				errCh <- fmt.Errorf("CSV read error: %v", err)
				return
			}

			cr, err := apiv2.NewIrisResultsTableRow(record)
			if err != nil {
				errCh <- fmt.Errorf("parse error: %v", err)
				continue
			}
			outCh <- *cr
		}
	}()

	return outCh, errCh
}

func arkResultsCmdArgs(cmd *cobra.Command, args []string) error {
	return nil
}

func arkResultsCmd(cmd *cobra.Command, args []string) {
	// nop
}

func streamArkResultsTableRow(sqlClient clientv2.SQLClient, query string) (<-chan apiv2.IrisResultsTableRow, <-chan error) {
	// setting this to a big number prevented the "connection reset by peer" error
	outCh := make(chan apiv2.IrisResultsTableRow, 10000)
	errCh := make(chan error, 1)

	go func() {
		defer close(outCh)

		stream, err := sqlClient.Download(query)
		if err != nil {
			errCh <- fmt.Errorf("download error: %v", err)
			return
		}
		defer stream.Close()

		stream2 := io.TeeReader(stream, os.Stdout)
		reader := csv.NewReader(stream2)
		// no need to skip header

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				errCh <- fmt.Errorf("CSV read error: %v", err)
				return
			}

			cr, err := apiv2.NewIrisResultsTableRow(record)
			if err != nil {
				errCh <- fmt.Errorf("parse error: %v", err)
				continue
			}
			outCh <- *cr
		}
	}()

	return outCh, errCh
}
