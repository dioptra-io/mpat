package v1

import (
	"dioptra-io/ufuk-research/pkg/client"
)

type irisClient struct {
	client.IrisClient

	username string
	password string
}

var _ client.IrisClient = (*irisClient)(nil)

func NewIrisClient(username, password string) client.IrisClient {
	return &irisClient{
		username: username,
		password: password,
	}
}
