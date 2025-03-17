package config

// These values are the default values used in the app.
const (
	DefaultNumWorkers = 32
	DefaultChunkSize  = 10000

	DefaultAfterTimeFlag   = ""
	DefaultBeforeTimeFlag  = ""
	DefaultForcedResetFlag = false
	DefaultOutputFlag      = ""
)

// Values for client and adapters
const ArkIPv4DatabaseBaseUrl = "https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily"
