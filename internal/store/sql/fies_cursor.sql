SELECT max(probe_dst_prefix)
FROM (
    SELECT DISTINCT probe_dst_prefix
    FROM {{.SourceTable}}
    WHERE valid_probe_protocol = 1
      AND private_probe_dst_prefix = 0
      AND probe_dst_prefix > toIPv6('{{.Cursor}}')
    ORDER BY probe_dst_prefix
    LIMIT {{.ChunkSize}}
)
HAVING count() > 0
