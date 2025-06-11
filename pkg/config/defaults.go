package config

import "time"

// These values are the default values used in the app.
const (
	DefaultParallelDownloads = 8
	DefaultNumWorkers        = 12
	DefaultChunkSize         = 100000
	DefaultUploadChunkSize   = 100000
	DefaultMaxRowUploadRate  = 0 // limitless
	DefaultSkipDuplicateIPs  = false
	DefaultMaxRetries        = 5
	DefaultStreamBufferSize  = 10000

	DefaultForcedResetFlag = false

	DefaultExponentialBackupCap = time.Second * 10 // 10 seconds max

	MaxIrisAPILimit = 200
)

// Values for client and adapters
const (
	DefaultArkIPv4DatabaseBaseURL = "https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily"
	DefaultIrisAPIURL             = "https://api.iris.dioptra.io"
)

const (
	DefaultIPv4Tag = "zeph-gcp-daily.json"
	DefaultIPv6Tag = "ipv6-hitlist.json"

	DefaultIrisctlJWTPath = ".iris/jwt"
)
