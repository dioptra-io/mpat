package copyiristables

import (
	"fmt"
	"math"

	"github.com/chigopher/pathlib"
	"github.com/spf13/cobra"

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

	fChunkSize    int
	fTableType    string
	fProgress     bool
	fStopOnError  bool
	fDownloadPath string
)

var logger = log.GetLogger()

var UtilCopyIrisTablesCmd = &cobra.Command{
	Use:   "copyiristables [measurement-uuids...]",
	Short: "This is a utility function that downloads and inserts iris tables and uploads them to the dev server.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		// If the log level is not debug disable info logs for preserving the progress bar.
		if fProgress && log.GetLogLevel() == log.LevelNormal {
			log.SetLogLevel(log.LevelSilent)
			logger.Debugln(
				"Flag --progress is set for displaying the progressbar changing the log level normal to log level silent.",
			)
		}
		prodConfig := DatabaseConfig{
			User:      fProdUser,
			Password:  fProdPassword,
			Host:      fProdHost,
			Database:  fProdDatabase,
			ChunkSize: fChunkSize,
			TableType: fTableType,
		}
		devConfig := DatabaseConfig{
			User:      fDevUser,
			Password:  fDevPassword,
			Host:      fDevHost,
			Database:  fDevDatabase,
			ChunkSize: fChunkSize,
			TableType: fTableType,
		}

		logger.Infof("Starting to download %v table(s).\n", len(args))
		downloadDirPath := pathlib.NewPath(fDownloadPath)

		resultsTableNames := make([]string, 0)
		for _, measUUID := range args {
			if tables, err := prodConfig.GetTablesOfMeasUUID(measUUID); err != nil {
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
			if numRows, err := prodConfig.GetSizeOfResultsTable(resultsTableName); err != nil {
				logger.Panicf("Cannot get size for table %s: %v\n", resultsTableName, err)
				return
			} else {
				logger.Debugf("Table %s has %v row(s).\n", resultsTableName, numRows)
				tableSizes = append(tableSizes, numRows)
				totalNumberOfChunks += int(math.Ceil(float64(numRows) / float64(fChunkSize)))
			}
		}

		for i, resultsTableName := range resultsTableNames {
			tableSize := tableSizes[i]

			// Create the table if it doesn't exists on the dev.
			if exists, err := devConfig.TableExists(resultsTableName); err != nil {
				logger.Panicf("Cannot create table %s: %v\n", resultsTableName, err)
				if fStopOnError {
					return
				}
			} else if exists {
				logger.Infof("Table %s already exists, skipping.\n", resultsTableName)
				continue
			}

			// Create the table if it doesn't exists on the dev.
			if err := devConfig.CreateResultsTable(); err != nil {
				logger.Panicf("Cannot create table %s: %v\n", resultsTableName, err)
				if fStopOnError {
					return
				}
			}

			// where to save the table
			tableDirPath := downloadDirPath.Join(resultsTableName)

			for j := 0; j < tableSize; j++ {
				chunkFilePath := tableDirPath.Parent().
					Join(fmt.Sprintf("%s__%d.temp", tableDirPath.Name(), j))

				logger.Infof(
					"Processing chunk %v/%v/%v %v\n",
					j,
					tableSize,
					totalNumberOfChunks,
					resultsTableName,
				)
				logger.Infof("Downloading %v\n", resultsTableName)

				// Download chunk
				if err := prodConfig.DownloadResultsTable(chunkFilePath); err != nil {
					logger.Panicf("Cannot download table chunk %s: %v\n", chunkFilePath.Name(), err)
					if fStopOnError {
						return
					}
				}

				logger.Infof("Uploading %v\n", resultsTableName)

				// Upload chunk
				if err := devConfig.UploadResultsTable(chunkFilePath); err != nil {
					logger.Panicf("Cannot download table chunk %s: %v\n", chunkFilePath.Name(), err)
					if fStopOnError {
						return
					}
				}

			}
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
		StringVar(&fTableType, "table-type", "Parquet", "this is the data type; CSV, Parquet etc.")
	UtilCopyIrisTablesCmd.Flags().
		BoolVar(&fProgress, "progress", false, "display a profress bar instead of logs.")
	UtilCopyIrisTablesCmd.Flags().
		BoolVar(&fStopOnError, "stop-on-error", false, "if set the program halts in case of an error.")
	UtilCopyIrisTablesCmd.Flags().
		StringVar(&fDownloadPath, "download-path", "data/downloads", "this is the temp directory for downloading the chunks.")
}
