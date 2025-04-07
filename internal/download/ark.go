package download

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/spf13/cobra"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

// flags
var (
	fSourceDSN string

	// fNumWorkers            int
	// fMaxRowUploadRate      int
	fChunkSize             int
	fForceResetDestination bool
	fInputFile             string
	// fMaxRetries            int
	// fSkipDuplicateIPs      bool
	// fUploadChunkSize       int

	fArkUsernamePassword string
)

var (
	inputArkDates []apiv1.Date
	arkCreds      apiv1.ArkCredentials
)

var logger = util.GetLogger()

func DownloadCmd() *cobra.Command {
	downloadCmd := &cobra.Command{
		Use:   "download <command>",
		Short: "Download the given tables to requested.",
		Long:  "Download table types from differeny sources.",
		Args:  downloadCmdArgs,
		Run:   downloadCmd,
	}
	downloadCmd.PersistentFlags().StringVarP(&fSourceDSN, "source-dsn", "s", "", "source database dsn string")
	downloadCmd.PersistentFlags().BoolVarP(&fForceResetDestination, "force-reset-destination", "f", config.DefaultForcedResetFlag, "truncate destination tables before copy")
	downloadCmd.PersistentFlags().StringVar(&fInputFile, "input-file", "", "file to read the table names")
	downloadCmd.Flags().IntVar(&fChunkSize, "chunk-size", config.DefaultChunkSize, "chunk size for the table")
	// downloadCmd.PersistentFlags().IntVarP(&fMaxRowUploadRate, "max-row-upload-rate", "r", config.DefaultMaxRowUploadRate, "limit the number of rows to upload per second")
	// downloadCmd.PersistentFlags().IntVar(&fMaxRetries, "max-retries", config.DefaultMaxRetries, "number of retries if a downlaod or upload fails")
	// downloadCmd.PersistentFlags().BoolVar(&fSkipDuplicateIPs, "skip-duplicate-ips", config.DefaultSkipDuplicateIPs, "perform group uniq array for route trace computation")
	// downloadCmd.PersistentFlags().IntVar(&fUploadChunkSize, "upload-chunk-size", config.DefaultUploadChunkSize, "chunk size for uploading")
	// downloadCmd.PersistentFlags().IntVar(&fNumWorkers, "num-workers", config.DefaultNumWorkers, "number of parallel workers and uploaders")

	downloadArkCmd := &cobra.Command{
		Use:   "ark <dates>...",
		Short: "Download the data for dates",
		Long:  "Download all of the data for all the given dates in ark dataset",
		Args:  downloadArkCmdArgs,
		Run:   downloadArkCmd,
	}

	downloadArkCmd.PersistentFlags().StringVarP(&fArkUsernamePassword, "ark-credentials", "c", "", "ark username:password")

	downloadCmd.AddCommand(downloadArkCmd)

	return downloadCmd
}

func downloadCmdArgs(cmd *cobra.Command, args []string) error {
	// nop
	return nil
}

func downloadCmd(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func downloadArkCmdArgs(cmd *cobra.Command, args []string) error {
	var inputArgs []string
	if fInputFile == "" {
		if len(args) <= 0 {
			cmd.Help()
			os.Exit(0)
		}

		inputArgs = args
	} else {
		if len(args) != 0 {
			return errors.New("cannot specify both input file and arguments")
		}
		f, err := os.Open(fInputFile)
		if err != nil {
			return err
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		inputArgs = make([]string, 0)
		for scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			inputArgs = append(inputArgs, scanner.Text())
		}
	}

	slices.Sort(inputArgs) // sort for easy compuation

	for i := 0; i < len(inputArgs); i++ {
		date, err := apiv1.ParseArkDate(inputArgs[i])
		if err != nil {
			return fmt.Errorf("error occured while parsing input dates: %v\n", err)
		}
		inputArkDates = append(inputArkDates, date)
	}

	cred, err := apiv1.ParseArkCredentials(fArkUsernamePassword)
	if err != nil {
		return fmt.Errorf("error occured while parsing ark credentials: %v\n", err)
	}
	arkCreds = cred

	logger.Infof("Preparing to process %d date(s).\n", len(inputArgs))

	return nil
}

func downloadArkCmd(cmd *cobra.Command, args []string) {
	// generate the result table names from dates
	resultTableNames := make([]apiv1.TableName, 0, len(inputArkDates))
	for i := 0; i < len(inputArkDates); i++ {
		resultTableNames = append(resultTableNames, inputArkDates[i].ToArkTableName())
	}

	logger.Debugf("Given table names to download: %v\n", resultTableNames)

	sourceClient, err := clientv1.NewSQLClient(fSourceDSN)
	if err != nil {
		logger.Fatalf("Error while connecting to source database: %v.\n", err)
		return
	}

	if err := sourceClient.HealthCheck(); err != nil {
		logger.Fatalf("Error on source database healthcheck: %v.\n", err)
		return
	}

	logger.Infoln("Database healthcheck is successful.")

	arkClient := clientv1.NewArkClient(arkCreds.Username, arkCreds.Pasword)

	// check if the given table names all exist on source.
	sourceTableInfos, err := sourceClient.GetTableInfoFromTableName(resultTableNames)
	if err != nil {
		logger.Fatalf("Error while checking source table info: %v.\n", err)
	}

	logger.Infoln("Validating tables on destination.")
	resultTablesToCopy := make([]apiv1.ResultsTableInfo, 0, len(inputArkDates))

	for i := 0; i < len(resultTableNames); i++ {
		sourceTableInfo := sourceTableInfos[i]

		if !sourceTableInfo.Exists || fForceResetDestination || sourceTableInfo.NumRows == 0 {
			if sourceTableInfo.Exists && !fForceResetDestination {
				if err := sourceClient.TruncateTableIfNotExists(sourceTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset source table: %v.\n", err)
					return
				}
			} else {
				if err := sourceClient.DropTableIfNotExists(sourceTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset source table: %v.\n", err)
					return
				}
				if err := sourceClient.CreateResultsTableIfNotExists(sourceTableInfo.TableName); err != nil {
					logger.Fatalf("Cannot reset source table: %v.\n", err)
					return
				}
			}
			logger.Debugf("Created source table: %v.\n", sourceTableInfo.TableName)

			resultTablesToCopy = append(resultTablesToCopy, sourceTableInfo) // add it to copy queue
		}
	}

	if len(resultTablesToCopy) == 0 {
		logger.Infof("Validated %d tables(s), all on destination are already valid. Exitting.\n", len(resultTableNames))
		return
	} else {
		logger.Infof("Validated %d tables(s), %d of them will be copied using.\n", len(resultTableNames), len(resultTablesToCopy))
	}

	logger.Debugf("Ark client: %v.\n", arkClient)
	// start pipeline
	panic("not implemented pipeline")
}
