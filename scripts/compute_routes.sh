#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

select_routes_query () {
    local database="$1"
    local results_table="$2"

    cat <<EOF
WITH
    toIPv6('::') AS null_ip,
    results_table as (
        SELECT
            probe_dst_addr,
            probe_src_addr,
            probe_dst_port,
            probe_src_port,
            probe_protocol,
            probe_ttl,
            reply_src_addr
        FROM 
            ${database}.${results_table}
    )
SELECT 
    DISTINCT ip_addr, dst_prefix, next_addr
FROM (
    WITH 
    -- Creates the range for the TTL
        groupUniqArray((probe_ttl, reply_src_addr)) as route_traces,
    -- Create the ttl_array and values_array
        arrayMap(x -> x.1, route_traces) as ttl_array,
        arrayMap(x -> x.2, route_traces) as address_array,
    -- Creates the range for the TTL
        range(toUInt8(arrayMin(ttl_array)), toUInt8(arrayMax(ttl_array) - 1)) as ttl_range,
    -- Convert the route traces to a map
        CAST((ttl_array, address_array), 'Map(UInt8, IPv6)') as route_traces_map,
    -- Create the links like in the links table calculation
        arrayMap(i -> (route_traces_map[toUInt8(i)], route_traces_map[toUInt8(i + 1)]), ttl_range) AS links,
    -- Filter out the null addresses
        arrayFilter(x -> x.1 <> null_ip and x.2 <> null_ip, links) as filtered_links,
    -- Join the links
        arrayJoin(filtered_links) AS link
    SELECT
        probe_dst_addr,
        probe_src_addr,
        toIPv6(cutIPv6(probe_dst_addr, 0, 1)) as dst_prefix,
        link.1 as ip_addr,
        link.2 as next_addr 
    FROM 
        results_table
    GROUP BY
        probe_dst_addr, 
        probe_src_addr,
        probe_dst_port,
        probe_src_port,
        probe_protocol
    ORDER BY 
        probe_dst_addr, 
        probe_src_addr,
        probe_dst_port,
        probe_src_port,
        probe_protocol
)
ORDER BY
    ip_addr,
    dst_prefix,
    next_addr
FORMAT Parquet
EOF
}

main () {
    local input
	local i
    local query
    local database
    local routes_directory

    # default value is the iris database
    database="iris"
    routes_directory="routes"

	if [[ $# -ne 1 ]]; then
		echo "$0: missing argument"
		return 1
	fi
	input="$1"

	i=0
	while read -r table; do
        query=$(select_routes_query "${database}" "${table}")
        routes_path="${routes_directory}/routes_${table}.parquet"

        curl -s \
            -X POST \
            --user "$PROD_CH_USER:$PROD_CH_PASSWD" \
            "$PROD_CH_URL" \
            -d "${query}" >> "${routes_path}"

        echo "downloaded " "${i}" "${table}"
        # echo "${query}" "${routes_path}" "${routes_folder}"

		i=$((i + 1))
	done < "$input"
}

main "$@"
