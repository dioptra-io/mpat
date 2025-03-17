package copyarkcmd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/query"
	"dioptra-io/ufuk-research/pkg/util"
)

type contextKey string

var (
	keyNumParallelDownloads contextKey = "keyNumParallelDownloads"
	keyChunkSize            contextKey = "keyChunkSize"
	keyForceTableReset      contextKey = "keyForceTableReset"
)

var logger = util.GetLogger()

var (
	fBefore          string
	fAfter           string
	fIrisAPIUser     string
	fIrisAPIPassword string
	fIrisAPIUrl      string

	fArkAPIUser     string
	fArkAPIPassword string

	fIrisResearchClickHouseDSN string

	fParallelDownloads int
	fChunkSize         int
	fForceTableReset   bool
)

var CopyArkCmd = &cobra.Command{
	Use:   "ark <datetimes...>",
	Short: "This command is used to copy the Ark data.",
	Long:  "...",
	Run: func(cmd *cobra.Command, args []string) {
		var datesToFetch []string
		fArkAPIUser := viper.GetString("ark-api-user")
		fArkAPIPassword := viper.GetString("ark-api-password")
		fIrisResearchClickHouseDSN := viper.GetString("iris-research-clickhouse-dsn")

		// Check if the before or after flags are given. If they are given ignore the arguments
		if len(fBefore) != 0 || len(fAfter) != 0 {
			panic("retrieval of the result tables are not supported yet.")
		} else {
			// Get the table names from arguments
			datesToFetch = args
		}

		irisClient := client.FromDSN(fIrisResearchClickHouseDSN)
		arkClient := client.FromParams(fArkAPIUser, fArkAPIPassword)

		logger.Infof("Connected to databases.\n")
		logger.Infof("Number of dates to copy is %d, using %d workers.\n", len(datesToFetch), fParallelDownloads)

		ctx := context.WithValue(context.Background(), keyNumParallelDownloads, fParallelDownloads)
		ctx = context.WithValue(ctx, keyChunkSize, fChunkSize)
		ctx = context.WithValue(ctx, keyForceTableReset, fForceTableReset)

		dateTimeList := make([]time.Time, len(datesToFetch))
		wartLinksList := make([][]string, len(datesToFetch))
		totalFilesToDownload := 0
		finishedNumberOfFiles := 0

		for i, dateString := range datesToFetch {
			dateTime, err := util.ParseDateTime(dateString)
			if err != nil {
				panic(err)
			}

			dateTimeList[i] = dateTime
			wartLinksList[i], err = getWartLinks(arkClient, dateTime)
			if err != nil {
				panic(err)
			}
			totalFilesToDownload += len(wartLinksList[i])
		}

		logger.Infof("Found total of %d wart files from %d day(s) to download by %d worker(s).\n", totalFilesToDownload, len(dateTimeList), fParallelDownloads)

		for i, t := range dateTimeList {
			wartURLs := wartLinksList[i]
			tableName := fmt.Sprintf("ark_resutls__cycle_%04d%02d%02d", t.Year(), t.Month(), t.Day())

			if err := run(ctx, arkClient, irisClient, t, wartURLs, totalFilesToDownload, finishedNumberOfFiles, tableName); err != nil {
				panic(err)
			}

			finishedNumberOfFiles += len(wartURLs)
		}
	},
}

func init() {
	CopyArkCmd.PersistentFlags().StringVar(&fBefore, "before", "", "before datetime")
	CopyArkCmd.PersistentFlags().StringVar(&fAfter, "after", "", "after datetime")
	CopyArkCmd.PersistentFlags().IntVar(&fChunkSize, "chunk-size", 10000, "use this to chunk the rows")
	CopyArkCmd.PersistentFlags().BoolVarP(&fForceTableReset, "force-table-reset", "f", false, "use this to recreate all of the tables")
	CopyArkCmd.PersistentFlags().IntVar(&fParallelDownloads, "parallel-downloads", 32, "use this to limit number of concurrent downloads")

	CopyArkCmd.PersistentFlags().StringVar(&fIrisResearchClickHouseDSN, "iris-research-clickhouse-dsn", "", "DSN string for research")
	CopyArkCmd.PersistentFlags().StringVar(&fArkAPIUser, "ark-api-user", "", "Ark API username")
	CopyArkCmd.PersistentFlags().StringVar(&fArkAPIPassword, "ark-api-password", "", "Ark API password")

	viper.BindPFlag("iris-research-clickhouse-dsn", CopyArkCmd.Flags().Lookup("iris-research-clickhouse-dsn"))
	viper.BindPFlag("ark-api-user", CopyArkCmd.Flags().Lookup("ark-api-user"))
	viper.BindPFlag("ark-api-password", CopyArkCmd.Flags().Lookup("ark-api-password"))

	viper.BindEnv("iris-research-clickhouse-dsn", "MPAT_IRIS_RESEARCH_DSN")
	viper.BindEnv("ark-api-user", "MPAT_ARK_API_USER")
	viper.BindEnv("ark-api-password", "MPAT_ARK_API_PASSWORD")
}

func getWartLinks(arkClient client.ArkClient, t time.Time) ([]string, error) {
	arkHTTPAdapter, err := arkClient.ArkHTTPAdapter(false)
	if err != nil {
		return nil, err
	}

	wartLinks, err := arkHTTPAdapter.WartLinks(t)
	if err != nil {
		return nil, err
	}

	return wartLinks, nil
}

func run(ctx context.Context, arkClient client.ArkClient, irisClient client.IrisClient, t time.Time, wartLinks []string, totalFilesToDownload, finishedNumberOfFiles int, tableName string) error {
	// Get the parameters from context
	numParallelDownloads := ctx.Value(keyNumParallelDownloads).(int)
	forceTableReset := ctx.Value(keyForceTableReset).(bool)

	arkHTTPAdapter, err := arkClient.ArkHTTPAdapter(false)
	if err != nil {
		return err
	}

	irisSQLAdapter, err := irisClient.ClickHouseSQLAdapter(false)
	if err != nil {
		return err
	}

	// Drop the table on the research if the flag forceTableReset is set.
	if forceTableReset {
		if _, err := irisSQLAdapter.Exec(query.DropTable(tableName, true)); err != nil {
			return err
		}
	}

	// Create the table if not exists
	if _, err := irisSQLAdapter.Exec(query.CreateResultsTable(tableName, true)); err != nil {
		return err
	}

	// agentNames := util.GetUniqueAgentNames(wartLinks)
	numWartFiles := len(wartLinks)

	// For performance optimization we are doing this using curl/http as a requests
	// instead of scanning the rows.
	lock := sync.Mutex{}
	localOrder := 0

	var wg sync.WaitGroup
	rateLimitedCh := make(chan int, numParallelDownloads)

	for j, wartLink := range wartLinks {
		wg.Add(1)
		rateLimitedCh <- j

		func() {
			if err := download(ctx,
				arkHTTPAdapter,
				irisSQLAdapter,
				wartLink,
				t,
				tableName); err != nil {
				<-rateLimitedCh
				wg.Done()
				return
			}
			<-rateLimitedCh
			wg.Done()

			lock.Lock()
			localOrder += 1

			percent := 100 * float64(localOrder) / float64(numWartFiles)
			totalPercent := 100 * float64(localOrder+finishedNumberOfFiles) / float64(totalFilesToDownload)
			logger.Infof("Processed wart (%v/%v %.2f%%)[%v/%v %.2f%%] for table %s.\n",
				localOrder+finishedNumberOfFiles,
				totalFilesToDownload,
				totalPercent,
				localOrder,
				numWartFiles,
				percent,
				tableName)
			lock.Unlock()
		}()

	}

	wg.Wait()
	return nil
}

func download(ctx context.Context, arkHTTPAdapter client.ArkHTTPAdapter, irisSQLAdapter client.ClickHouseSQLAdapter, wartLink string, t time.Time, tableName string) error {
	// // Get the parameters from context
	chunkSize := ctx.Value(keyChunkSize).(int)

	wartDownloader, err := arkHTTPAdapter.Download(wartLink, t)
	if err != nil {
		return err
	}
	defer wartDownloader.Close()

	unzipper, err := client.NewGZipConverter().Convert(wartDownloader)
	if err != nil {
		return err
	}
	defer unzipper.Close()

	pantracer, err := client.NewPantraceConverter(logger).Convert(unzipper)
	if err != nil {
		return err
	}
	defer pantracer.Close()

	recordCh, errCh := client.NewPantraceToProbeRecordConverter(chunkSize, true).Convert(pantracer)

	recordBuffer := make([]client.ProbeRecord, 0)

	shouldContinue := true
	for shouldContinue {
		select {
		case record, ok := <-recordCh:
			if !ok {
				shouldContinue = false
				continue
			} else {
				// Add the probe ubuffer to the record buffer
				recordBuffer = append(recordBuffer, record)

				// If we reached the chunk size then insert recods to Clickhouse.
				if cap(recordBuffer) == chunkSize {
					err = insert(irisSQLAdapter, recordBuffer, tableName)
					if err != nil {
						return err
					}
					recordBuffer = make([]client.ProbeRecord, 0)
				}
			}
		case err, ok := <-errCh:
			if !ok && err != nil {
				return err
			}
		}
	}

	// Insert the remeaning recids to Clickhouse
	err = insert(irisSQLAdapter, recordBuffer, tableName)
	if err != nil {
		return err
	}

	return nil
}

// Expected a slice thus, the cap is checked.
func insert(irisSQLAdapter client.ClickHouseSQLAdapter, recordsToInsert []client.ProbeRecord, tableName string) error {
	tx, err := irisSQLAdapter.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query.InsertResultsWithoutMPLSLables(tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range recordsToInsert {
		stmt.Exec(
			record.CaptureTimestamp,
			record.ProbeProtocol,
			record.ProbeSrcAddr.String(),
			record.ProbeDstAddr.String(),
			record.ProbeSrcPort,
			record.ProbeDstPort,
			record.ProbeTTL,
			record.QuotedTTL,
			record.ReplySrcAddr.String(),
			record.ReplyProtocol,
			record.ReplyICMPType,
			record.ReplyICMPCode,
			record.ReplyTTL,
			record.ReplySize,
			record.RTT,
			record.Round,
		)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
