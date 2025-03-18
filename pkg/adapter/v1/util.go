package v1

import (
	"io"

	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/util"
)

func ComputeRouteScoresTable(irisCHClient client.DBClient,
	resultTableNames,
	routesTableNames []string,
	chunkSize,
	numWorkers int,
	forceTableReset bool,
) error {
	logger := util.GetLogger()

	logger.Infof("Number of results tables to copy is %d, using %d workers.\n", len(resultTableNames), numWorkers)

	if !forceTableReset {
		logger.Infoln("Force table reset flag is set to false, skipping route table computation.")
		return nil
	} else {
		logger.Infoln("Force table reset flag is set to true, commencing route table computation.")
	}

	numTables := len(resultTableNames)

	// Compute the route tables one by one.
	for i := 0; i < numTables; i++ {
		resultTableName := resultTableNames[i]
		routesTableName := routesTableNames[i]

		// Get the result table to upload everyting
		precent := float64(i) / float64(numTables)
		logger.Infof("Processing [%v/%v %v%%] from %s to %s.\n",
			i,
			numTables,
			precent,
			resultTableName,
			routesTableName)

		// Get the route traces from the merge
		streamer := NewRouteTraceChunkSreamer(irisCHClient, chunkSize, []string{resultTableName})
		processor := NewRouteTraceChunkProcessor(chunkSize, numWorkers)
		uploader, err := NewRouteRecordUploader(irisCHClient, chunkSize, routesTableName, forceTableReset)
		if err != nil {
			panic(err)
		}

		streamCh, errCh := streamer.Stream()
		streamCh1, errCh1 := processor.Process(streamCh, errCh)
		doneCh, errCh3 := uploader.Upload(streamCh1, errCh1)

		select {
		case _, ok := <-doneCh:
			if ok {
				logger.Debugln("Done processing")
			}
		case err, ok := <-errCh3:
			if ok {
				panic(err)
			}
		}
	}

	return nil
}

func WriteToFile(w io.Writer, irisClient client.DBClient) error {
	panic("not implemented")
	return nil
}
