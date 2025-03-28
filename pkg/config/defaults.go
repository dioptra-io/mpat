package config

import "time"

// These values are the default values used in the app.
const (
	DefaultParallelDownloads = 8
	DefaultChunkSize         = 100000
	DefaultMaxRowUploadRate  = 0 // limitless
	DefaultMaxRetries        = 5

	DefaultForcedResetFlag = false

	DefaultExponentialBackupCap = time.Second * 10 // 10 seconds max
)

// Values for client and adapters
const ArkIPv4DatabaseBaseUrl = "https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily"
