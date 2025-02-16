package copyiristables

import (
	"math"
	"sync"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/log"
)

var (
	fProdUser     string
	fProdPassword string
	fProdDatabase string
	fProdHost     string

	fDevUser     string
	fDevPassword string
	fDevDatabase string
	fDevHost     string

	fChunkSize         int
	fTransferFormat    string
	fProgress          bool
	fStopOnError       bool
	fForceTruncate     bool
	fParallelDownloads int
)

var logger = log.GetLogger()

var UtilCopyIrisTablesCmd = &cobra.Command{
	Use:   "copyiristables [measurement-uuids...]",
	Short: "This is a utility function that downloads and inserts iris tables and uploads them to the dev server.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		// If the log level is not debug disable info logs for preserving the progress bar.
		if fProgress && log.GetLogLevel() != log.LevelDebug {
			log.SetLogLevel(log.LevelSilent)
			logger.Debugln(
				"Flag --progress is set for displaying the progressbar changing the log level normal to log level silent.",
			)
		}

		prodConfig := DatabaseConfig{
			User:      viper.GetString("prod-user"),
			Password:  viper.GetString("prod-password"),
			Host:      viper.GetString("prod-url"),
			Database:  viper.GetString("prod-database"),
			ChunkSize: fChunkSize,
			TableType: fTransferFormat,
		}
		devConfig := DatabaseConfig{
			User:      viper.GetString("dev-user"),
			Password:  viper.GetString("dev-password"),
			Host:      viper.GetString("dev-url"),
			Database:  viper.GetString("dev-database"),
			ChunkSize: fChunkSize,
			TableType: fTransferFormat,
		}

		resultsTableNames := make([]string, 0)
		for _, measUUID := range args {
			if tables, err := prodConfig.GetTablesForMeasurementUUID(measUUID); err != nil {
				logger.Panicf("Cannot get tables for measurement-uuid %s: %v\n", measUUID, err)
				return
			} else {
				logger.Debugf("Measurement %s has %v table(s).\n", measUUID, len(tables))
				resultsTableNames = append(resultsTableNames, tables...)
			}
		}

		logger.Debugf("Will process tables: %v\n", resultsTableNames)

		tableSizes := make([]int, 0)
		totalNumberOfChunks := 0
		for _, resultsTableName := range resultsTableNames {
			if numRows, err := prodConfig.GetTableSize(resultsTableName); err != nil {
				logger.Panicf("Cannot get size for table %s: %v\n", resultsTableName, err)
				return
			} else {
				logger.Debugf("Table %s has %v row(s).\n", resultsTableName, numRows)
				tableSizes = append(tableSizes, numRows)
				totalNumberOfChunks += int(math.Ceil(float64(numRows) / float64(fChunkSize)))
			}
		}

		progressBar := progressbar.Default(int64(totalNumberOfChunks), "Copying results tables")

		logger.Infof("Starting to download %v table(s).\n", totalNumberOfChunks)
		logger.Debugf("Table sizes: %v\n", tableSizes)

		for i, resultsTableName := range resultsTableNames {
			tableSize := tableSizes[i]

			logger.Infof("Processing started for table %s.\n", resultsTableName)

			// Create the table
			if exists, err := devConfig.CreateResultsTableIfNotExists(resultsTableName); err != nil {
				logger.Panicf("Cannot create table %s: %v.\n", resultsTableName, err)
				return
			} else if exists {
				if fForceTruncate {
					logger.Infof("Table %s already exists, truncating.\n", resultsTableName)
					if err := devConfig.TruncateTable(resultsTableName); err != nil {
						logger.Panicf("Cannot truncate table %s: %v.\n", resultsTableName, err)
						return
					}
				} else {
					logger.Infof("Table %s already exists, skipping.\n", resultsTableName)
					progressBar.Add(tableSize)
					continue
				}
			}

			numChunks := int(math.Ceil(float64(tableSize) / float64(fChunkSize)))

			var wg sync.WaitGroup
			rateLimitedCh := make(chan int, fParallelDownloads)

			for j := 0; j < numChunks; j++ {
				chunk := fChunkSize * j
				logger.Debugf(
					"Downloading chunk %v/%v for table %s.\n",
					j,
					numChunks,
					resultsTableName,
				)

				wg.Add(1)
				rateLimitedCh <- j

				go func() {
					logger.Debugf("Started worker %v.\n", j)
					if err := Transfer(
						&prodConfig,
						&devConfig,
						resultsTableName,
						chunk,
						numChunks,
						progressBar,
						j,
						fProgress,
					); err != nil {
						<-rateLimitedCh
						wg.Done()
						logger.Panicf("error on transfer: %s\n", err)
						if fStopOnError {
							return
						}
					}
					<-rateLimitedCh
					wg.Done()
				}()

			}

			wg.Wait()
		}

		logger.Infoln("Uploaded all tables.")
	},
}

func init() {
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fProdUser, "prod-user", "", "this is the username of the prod.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fProdPassword, "prod-password", "", "this is the password of the prod.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fProdDatabase, "prod-database", "iris", "this flag is the default prod database.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fProdHost, "prod-url", "https://chproxy.iris.dioptra.io", "this is the hostname of the prod server.")

	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fDevUser, "dev-user", "admin", "this is the username of the dev.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fDevPassword, "dev-password", "", "this is the password of the dev.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fDevDatabase, "dev-database", "iris", "this flag is the default dev database.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fDevHost, "dev-url", "localhost:9000", "this is the hostname of the dev server.")

	UtilCopyIrisTablesCmd.Flags().
		IntVar(&fChunkSize, "chunk-size", 100000, "this is the size of the chunks")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fTransferFormat, "transfer-format", "Native", "this is the data type; CSV, Parquet etc.")
	UtilCopyIrisTablesCmd.Flags().
		BoolVar(&fProgress, "progress", false, "display a profress bar instead of logs.")
	UtilCopyIrisTablesCmd.Flags().
		BoolVar(&fStopOnError, "stop-on-error", false, "if set the program halts in case of an error.")
	UtilCopyIrisTablesCmd.Flags().
		BoolVar(&fForceTruncate, "force-truncate", false, "this flag is used to truncate the existing tables.")
	UtilCopyIrisTablesCmd.Flags().
		IntVar(&fParallelDownloads, "parallel-downloads", 16, "this flag sets the parallel number of downloads")

	viper.BindPFlag("prod-user", UtilCopyIrisTablesCmd.Flags().Lookup("prod-user"))
	viper.BindPFlag("prod-password", UtilCopyIrisTablesCmd.Flags().Lookup("prod-password"))
	viper.BindPFlag("prod-database", UtilCopyIrisTablesCmd.Flags().Lookup("prod-database"))
	viper.BindPFlag("prod-url", UtilCopyIrisTablesCmd.Flags().Lookup("prod-url"))

	viper.BindPFlag("dev-user", UtilCopyIrisTablesCmd.Flags().Lookup("dev-user"))
	viper.BindPFlag("dev-password", UtilCopyIrisTablesCmd.Flags().Lookup("dev-password"))
	viper.BindPFlag("dev-database", UtilCopyIrisTablesCmd.Flags().Lookup("dev-database"))
	viper.BindPFlag("dev-url", UtilCopyIrisTablesCmd.Flags().Lookup("dev-url"))

	viper.BindEnv("prod-user", "MPAT_PROD_USER")
	viper.BindEnv("prod-password", "MPAT_PROD_PASSWORD")
	viper.BindEnv("prod-database", "MPAT_PROD_DATABASE")
	viper.BindEnv("prod-url", "MPAT_PROD_HOST")

	viper.BindEnv("dev-user", "MPAT_DEV_USER")
	viper.BindEnv("dev-password", "MPAT_DEV_PASSWORD")
	viper.BindEnv("dev-database", "MPAT_DEV_DATABASE")
	viper.BindEnv("dev-url", "MPAT_DEV_HOST")
}

func Transfer(
	prodConfig, devConfig *DatabaseConfig,
	resultsTableName string,
	chunk, numChunks int,
	progressBar *progressbar.ProgressBar,
	j int,
	enableProgressBar bool,
) error {
	// Start transfer
	reader, err := prodConfig.DownloadTable(resultsTableName, chunk)
	if err != nil {
		logger.Panicf("Cannot download table %s: %v\n", resultsTableName, err)
		return err
	}

	logger.Debugf(
		"Uploading chunk %v/%v for table %s.\n",
		j,
		numChunks,
		resultsTableName,
	)

	err = devConfig.UploadTable(resultsTableName, reader)
	if err != nil {
		logger.Panicf("Cannot upload table %s: %v\n", resultsTableName, err)
		return err
	}

	reader.Close()
	logger.Infof(
		"Processing done for chunk %v/%v for table %s.\n",
		j,
		numChunks,
		resultsTableName,
	)

	if enableProgressBar {
		progressBar.Add(1)
	}
	return nil
}
