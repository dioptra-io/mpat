package client

import (
	"database/sql"
	"net/url"
)

type IrisClientConfig struct {
	DSN string
}

type basicIrisClient struct {
	IrisClient
	cfg         IrisClientConfig
	sqlAdapter  ClickHouseSQLAdapter
	httpAdapter ClickHouseHTTPAdapter
}

func FromConfig(cfg IrisClientConfig) IrisClient {
	return &basicIrisClient{
		cfg:         cfg,
		sqlAdapter:  nil,
		httpAdapter: nil,
	}
}

func FromDSN(dsn string) IrisClient {
	return FromConfig(IrisClientConfig{DSN: dsn})
}

func (c *basicIrisClient) ClickHouseSQLAdapter(reOpenIfExists bool) (ClickHouseSQLAdapter, error) {
	if c.sqlAdapter != nil && !reOpenIfExists {
		return c.sqlAdapter, nil
	} else if c.sqlAdapter != nil && reOpenIfExists {
		if err := c.sqlAdapter.Close(); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("clickhouse", c.cfg.DSN)
	if err != nil {
		return nil, err
	}
	c.sqlAdapter = db
	return c.sqlAdapter, nil
}

func (c *basicIrisClient) ClickHouseHTTPAdapter(reOpenIfExists bool) (ClickHouseHTTPAdapter, error) {
	if c.httpAdapter != nil && !reOpenIfExists {
		return c.httpAdapter, nil
	} else if c.httpAdapter != nil && reOpenIfExists {
		if err := c.httpAdapter.Close(); err != nil {
			return nil, err
		}
	}
	parsedURL, err := url.Parse(c.cfg.DSN)
	if err != nil {
		return nil, err
	}
	c.httpAdapter = newBasicClickHouseHTTPAdapter(parsedURL)
	return c.httpAdapter, nil
}

// Similar thing for Ark
type ArkClientConfig struct {
	Username string
	Password string
}

type basicArkClient struct {
	ArkClient
	cfg         ArkClientConfig
	httpAdapter ArkHTTPAdapter
}

func FromArkConfig(cfg ArkClientConfig) ArkClient {
	return &basicArkClient{
		cfg:         cfg,
		httpAdapter: nil,
	}
}

func FromParams(username, password string) ArkClient {
	return FromArkConfig(ArkClientConfig{
		Username: username,
		Password: password,
	})
}

func (c *basicArkClient) ArkHTTPAdapter(reOpenIfExists bool) (ArkHTTPAdapter, error) {
	if c.httpAdapter != nil && !reOpenIfExists {
		return c.httpAdapter, nil
	} else if c.httpAdapter != nil && reOpenIfExists {
		if err := c.httpAdapter.Close(); err != nil {
			return nil, err
		}
	}
	c.httpAdapter = newBasicClickHouseHTTPAdapter(parsedURL)
	return c.httpAdapter, nil
}
