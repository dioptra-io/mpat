package arkdata

import (
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dioptra-io/ufuk-research/internal/common"
	"dioptra-io/ufuk-research/internal/log"
)

var (
	fArkUser     string
	fArkPassword string
	fArkURL      string

	fDevUser     string
	fDevPassword string
	fDevDatabase string
	fDevHost     string

	fProgress          bool
	fStopOnError       bool
	fForceDelete       bool
	fParallelDownloads int
)

var logger = log.GetLogger()

var CopyArkDataCmd = &cobra.Command{
	Use:   "arkdata [start-time] [end_time]",
	Short: "This is a utility function that downloads and inserts iris tables and uploads them to the dev server.",
	Long:  "...",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		startTime, err := time.Parse("2006-01-02", args[0])
		if err != nil {
			logger.Panicf("Error occured while parsing the start time: %s.\n", err)
		}

		endTime, err := time.Parse("2006-01-02", args[1])
		if err != nil {
			logger.Panicf("Error occured while parsing the end time: %s.\n", err)
		}

		arkClient := &ArkClient{
			BaseURL:     viper.GetString("ark-url"),
			ArkUser:     viper.GetString("ark-user"),
			ArkPassword: viper.GetString("ark-password"),
			StartTime:   startTime,
			EndTime:     endTime,
			Index:       0,
		}

		conn, err := common.NewConnection()
		if err != nil {
			logger.Panicf("Error while connecting to clickhouse: %v.\n", err)
			return
		}

		logger.Debugf("Getting cycles between %s and %s.\n", startTime, endTime)
		logger.Infof("There given the dates %v and %v, there are %v cycles.\n", startTime, endTime, arkClient.Length())

		for arkClient.Next() {
			wartLinks, err := arkClient.CurrentWartLinks()
			if err != nil && fStopOnError {
				logger.Panicf("Error occured when downloading the list of wart files, stop on error flag is set so exitting: %v.\n", err)
				return
			} else if err != nil && !fStopOnError {
				logger.Panicf("Error occured when downloading the list of agents, stop on error flag is not set so continuing: %v.\n", err)
				continue
			}

			wartLinks = []string{wartLinks[0]}

			currentCycleString := arkClient.CurrentCycleTableString()

			logger.Infof("For the %v (%v/%v), there are %v wart files.", currentCycleString, arkClient.Index, arkClient.Length(), len(wartLinks))

			var wg sync.WaitGroup
			rateLimiterCh := make(chan int, fParallelDownloads)
			errorCh := make(chan error, len(wartLinks))

			for i := 0; i < len(wartLinks); i++ {
				wg.Add(1)
				rateLimiterCh <- i

				go func(i int) {
					defer wg.Done()
					err := run(conn, arkClient, wartLinks[i], i, fParallelDownloads, len(wartLinks))
					<-rateLimiterCh
					errorCh <- err
				}(i)
			}

			go func() {
				wg.Wait()
				close(errorCh)
			}()

			for err := range errorCh {
				if err != nil && fStopOnError {
					logger.Panicf("Error occured on the wart pipeline, stop on error flag set so exitting: %v.\n", err)
					return
				} else if err != nil && !fStopOnError {
					logger.Panicf("Error occured on the wart pipeline, stop on error flag is not set so continuing: %v.\n", err)
					continue
				}
			}
			logger.Infof("Cycle %v (%v/%v) is finished!\n", currentCycleString, arkClient.Index, arkClient.Length())
		}
		logger.Infof("All cycles are finished!\n")
	},
}

func init() {
	CopyArkDataCmd.Flags().
		StringVar(&fArkUser, "ark-user", "", "this is the username of the ark.")
	CopyArkDataCmd.Flags().
		StringVar(&fArkPassword, "ark-password", "", "this is the password of the ark.")
	CopyArkDataCmd.Flags().
		StringVar(&fArkURL, "ark-url", "", "this is the url of the ark.")

	CopyArkDataCmd.Flags().
		StringVar(&fDevUser, "dev-user", "admin", "this is the username of the dev.")
	CopyArkDataCmd.Flags().
		StringVar(&fDevPassword, "dev-password", "", "this is the password of the dev.")
	CopyArkDataCmd.Flags().
		StringVar(&fDevDatabase, "dev-database", "iris", "this flag is the default dev database.")
	CopyArkDataCmd.Flags().
		StringVar(&fDevHost, "dev-url", "localhost:9000", "this is the hostname of the dev server.")

	CopyArkDataCmd.Flags().
		BoolVar(&fProgress, "progress", false, "display a profress bar instead of logs.")
	CopyArkDataCmd.Flags().
		BoolVar(&fStopOnError, "stop-on-error", false, "if set the program halts in case of an error.")
	CopyArkDataCmd.Flags().
		BoolVar(&fForceDelete, "force-delete", false, "this flag is used to delete the existing tables.")
	CopyArkDataCmd.Flags().
		IntVar(&fParallelDownloads, "parallel-downloads", 16, "this flag sets the parallel number of downloads")

	viper.BindPFlag("ark-user", CopyArkDataCmd.Flags().Lookup("ark-user"))
	viper.BindPFlag("ark-password", CopyArkDataCmd.Flags().Lookup("ark-password"))
	viper.BindPFlag("ark-url", CopyArkDataCmd.Flags().Lookup("ark-url"))

	viper.BindPFlag("dev-user", CopyArkDataCmd.Flags().Lookup("dev-user"))
	viper.BindPFlag("dev-password", CopyArkDataCmd.Flags().Lookup("dev-password"))
	viper.BindPFlag("dev-database", CopyArkDataCmd.Flags().Lookup("dev-database"))
	viper.BindPFlag("dev-url", CopyArkDataCmd.Flags().Lookup("dev-url"))

	viper.BindEnv("ark-user", "MPAT_ARK_USER")
	viper.BindEnv("ark-password", "MPAT_ARK_PASSWORD")
	viper.BindEnv("ark-url", "MPAT_ARK_URL")

	viper.BindEnv("dev-user", "MPAT_DEV_USER")
	viper.BindEnv("dev-password", "MPAT_DEV_PASSWORD")
	viper.BindEnv("dev-database", "MPAT_DEV_DATABASE")
	viper.BindEnv("dev-url", "MPAT_DEV_HOST")
}

func run(conn clickhouse.Conn, arkClient *ArkClient, wartLink string, i, numParallel, numFiles int) error {
	logger.Infof("Started downloading wart file %q.\n", wartLink)
	downloadReadCloser, err := arkClient.DownloadRawFile(wartLink)
	if err != nil {
		return err
	}
	defer downloadReadCloser.Close()

	logger.Infof("Started decompressing wart file %q.\n", wartLink)
	decompressReadCloser, err := arkClient.DecompressRawFile(downloadReadCloser)
	if err != nil {
		return err
	}
	defer decompressReadCloser.Close()

	logger.Infof("Started converting wart file %q.\n", wartLink)
	convertedReadCloser, err := arkClient.ConvertDecompressedFile(decompressReadCloser)
	if err != nil {
		return err
	}
	defer convertedReadCloser.Close()

	logger.Infof("Started uploading wart file %q.\n", wartLink)
	err = arkClient.UploadConvertedFile(conn, convertedReadCloser, "ark", "ark_cycle_1") // TODO XXX
	if err != nil {
		return err
	}

	// data, err := io.ReadAll(convertedReadCloser)
	// if err != nil {
	// 	return err
	// }
	//
	// fmt.Printf("string(data): %v\n", string(data))

	logger.Infof("Ark pipeline %v/%v is finished!\n", i+1, numFiles)
	return nil
}
