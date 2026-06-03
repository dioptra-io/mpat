CREATE TABLE IF NOT EXISTS {{.Database}}.{{.Table}} (
    `capture_timestamp` DateTime CODEC(T64, ZSTD(1)),
    `probe_protocol`    UInt8,
    `probe_src_addr`    IPv6,
    `probe_dst_addr`    IPv6,
    `probe_src_port`    UInt16,
    `probe_dst_port`    UInt16,
    `probe_ttl`         UInt8,
    `reply_src_addr`    IPv6,
    `rtt`               UInt16 CODEC(T64, ZSTD(1)),

    `probe_dst_prefix`  IPv6 MATERIALIZED toIPv6(cutIPv6(probe_dst_addr, 8, 0))
)
ENGINE = MergeTree
ORDER BY (
    probe_dst_prefix,
    probe_src_addr,
    probe_protocol,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    probe_ttl
)
SETTINGS
    index_granularity = 8192;
