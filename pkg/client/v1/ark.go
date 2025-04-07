package v1

import (
	"context"
	"io"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
)

type ArkClient struct {
	username string
	password string
}

func NewArkClient(username, password string) *ArkClient {
	return &ArkClient{
		username: username,
		password: password,
	}
}

func (c *ArkClient) GetArkCycles(ctx context.Context, dates []apiv1.Date) ([]apiv1.ArkCycle, error) {
	return nil, nil
}

// this downloads the wart files and unzips it using gzip
func (c *ArkClient) DownloadUnzippedWartFile(ctx context.Context, wart apiv1.WartFile) (io.ReadCloser, error) {
	return nil, nil
}

// func (c *ArkClient) GetCyclesFor(ctx context.Context, dates []time.Time) ([]apiv1.ArkCycle, error) {
// 	arkCycles := make([]apiv1.ArkCycle, 0)
// 	for _, date := range dates {
// 		arkCyclesTemp := apiv1.ArkCycleFromTime(date)
// 		arkCycles = append(arkCycles, *arkCyclesTemp)
// 	}
// 	return arkCycles, nil
// }
//
// func (c *ArkClient) GetCyclesBetween(ctx context.Context, after, before time.Time) ([]apiv1.ArkCycle, error) {
// 	times := util.GetDatesBetween(after, before)
// 	return c.GetCyclesFor(ctx, times)
// }
//
// func (c *ArkClient) GetWartfile(ctx context.Context, cycle apiv1.ArkCycle) ([]apiv1.ArkWartFile, error) {
// 	req, err := http.NewRequest("GET", cycle.URL, nil)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	req.SetBasicAuth(c.username, c.password)
//
// 	client := &http.Client{}
//
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	defer resp.Body.Close()
//
// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return nil, err
// 	}
// 	content := string(body)
//
// 	re := regexp.MustCompile("\".*gz\"")
//
// 	matches := re.FindAllString(content, -1)
//
// 	if len(matches) == 0 {
// 		return nil, fmt.Errorf("no match for the cycle-page")
// 	}
//
// 	arkWartFiles := make([]apiv1.ArkWartFile, 0)
//
// 	for i := 0; i < len(matches); i++ {
// 		wartFilename := strings.ReplaceAll(matches[i], "\"", "")
// 		arkWartFile := apiv1.ArkWartFromTime(cycle.Date, wartFilename)
// 		arkWartFiles = append(arkWartFiles, *arkWartFile)
// 	}
//
// 	return arkWartFiles, nil
// }
//
// func (c *ArkClient) DownloadWart(ctx context.Context, wartFile apiv1.ArkWartFile) (io.ReadCloser, error) {
// 	req, err := http.NewRequest("GET", wartFile.URL, nil)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	req.SetBasicAuth(c.username, c.password)
//
// 	cli := &http.Client{}
// 	resp, err := cli.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return resp.Body, nil
// }
