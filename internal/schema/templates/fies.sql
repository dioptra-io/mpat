CREATE TABLE IF NOT EXISTS {{.Database}}.{{.Table}} (
    `sequence_number`         UInt64,
    `agent_id`                IPv6,
    `probing_directive_id`    UInt32,
    `ip_version`              UInt8,
    `protocol`                UInt8,
    `source_address`          IPv6,
    `destination_address`     IPv6,
    `near_probe_ttl`          UInt8,
    `near_reply_address`      IPv6,
    `near_sent_timestamp`     DateTime,
    `near_received_timestamp` DateTime,
    `far_probe_ttl`           UInt8,
    `far_reply_address`       IPv6,
    `far_sent_timestamp`      DateTime,
    `far_received_timestamp`  DateTime,
    `production_timestamp`    DateTime
)
ENGINE = MergeTree
ORDER BY (
	near_reply_address, 
	destination_address, 
	agent_id, 
	production_timestamp
)
SETTINGS 
	index_granularity = 8192;
