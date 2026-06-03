INSERT INTO {{.Database}}.{{.Table}}
WITH aggregated AS (
    SELECT
        probe_protocol,
        probe_src_addr,
        probe_dst_prefix,
        probe_dst_addr,
        probe_src_port,
        probe_dst_port,
        probe_ttl,
        round,
        groupUniqArray(reply_src_addr) AS reply_addrs,
        min(capture_timestamp)         AS capture_timestamp,
        min(rtt)                       AS rtt
    FROM {{.SourceTable}}
    WHERE probe_dst_prefix IN (
        SELECT DISTINCT probe_dst_prefix
        FROM {{.SourceTable}}
        WHERE valid_probe_protocol = 1
          AND private_probe_dst_prefix = 0
          AND probe_dst_prefix > toIPv6('{{.Cursor}}')
        ORDER BY probe_dst_prefix
        LIMIT {{.ChunkSize}}
    )
    AND valid_probe_protocol = 1
    AND private_probe_dst_prefix = 0
    GROUP BY
        probe_protocol, probe_src_addr, probe_dst_prefix,
        probe_dst_addr, probe_src_port, probe_dst_port,
        probe_ttl, round
)
SELECT
    rowNumberInAllBlocks() + (SELECT count() FROM {{.Database}}.{{.Table}}) AS sequence_number,
    near.probe_src_addr                                                       AS agent_id,
    0                                                                         AS probing_directive_id,
    if(isIPv4String(toString(near.probe_dst_addr)), 4, 6)                     AS ip_version,
    near.probe_protocol                                                       AS protocol,
    near.probe_src_addr                                                       AS source_address,
    near.probe_dst_addr                                                       AS destination_address,
    near.probe_ttl                                                            AS near_probe_ttl,
    near.reply_addrs[1]                                                       AS near_reply_address,
    near.capture_timestamp                                                    AS near_sent_timestamp,
    near.capture_timestamp + INTERVAL (near.rtt * {{.RTTResolution}}) MILLISECOND AS near_received_timestamp,
    far.probe_ttl                                                             AS far_probe_ttl,
    far.reply_addrs[1]                                                        AS far_reply_address,
    far.capture_timestamp                                                     AS far_sent_timestamp,
    far.capture_timestamp + INTERVAL (far.rtt * {{.RTTResolution}}) MILLISECOND  AS far_received_timestamp,
    now()                                                                     AS production_timestamp
FROM aggregated AS near
JOIN aggregated AS far
    ON  near.probe_protocol = far.probe_protocol
    AND near.probe_src_addr = far.probe_src_addr
    AND near.probe_dst_prefix = far.probe_dst_prefix
    AND near.probe_dst_addr = far.probe_dst_addr
    AND near.probe_src_port = far.probe_src_port
    AND near.probe_dst_port = far.probe_dst_port
    AND near.round = far.round
    AND far.probe_ttl = near.probe_ttl + 1
WHERE length(near.reply_addrs) = 1
  AND length(far.reply_addrs) = 1
