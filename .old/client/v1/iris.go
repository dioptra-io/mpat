package v1

type IrisClient struct {
	username string
	password string
}

func NewIrisClient(username, password string) *IrisClient {
	return &IrisClient{
		username: username,
		password: password,
	}
}
