#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

readonly IRIS_DEV_HOSTNAME="ple1.planet-lab.eu"
readonly LOCAL_DIR="data" # downloaded tables saved here
: "${CHUNK_SIZE:=1000000}" # rows

main() {
	local input
	local i
	local num_rows
    local num_chunks

	if [[ $# -ne 1 ]]; then
		echo "$0: missing argument"
		return 1
	fi
	input="$1"

	i=0
	while read -r table; do
        num_rows=$(get_number_of_rows "$i" "$table")
        num_chunks=$((num_rows / CHUNK_SIZE))

		download_prod_table "$i" "$table" "$num_rows" "$num_chunks"
		upload_dev_table "$i" "$table" "$num_chunks"
		i=$((i + 1))
	done < "$input"
	wait 
	echo_log "Finished"
}

get_number_of_rows() {
	local process_id="$1"
	local table="$2"
	local num_rows
	local num_chunks

	# compute the number of chunks
	num_rows=$(curl -X POST -s --user "$PROD_CH_USER:$PROD_CH_PASSWD" -d "SELECT count(*) FROM ${PROD_CH_DB}.$table FORMAT CSV" "$PROD_CH_URL" --output -)

    echo "$num_rows"
}

#
# This function downloads a table from the Iris production server
# in chunks. The reason it downloads it in chunks has to do with
# a size limit when uploading it to the Iris development server.
#
download_prod_table() {
	local process_id="$1"
	local table="$2"
	local num_rows="$3"
	local num_chunks="$4"
    local chunk_of
    local i

	echo_log "P$process_id  starting to download $table"
    i=0
	for chunk_num in $(seq 0 1 "$num_chunks"); do
        chunk_of=$(calculate_chunk_of "$num_chunks" "$i")
		download_prod_table_chunk "$process_id" "$table" "$chunk_num" "$num_chunks" "$chunk_of"
        i=$((i + 1))
	done
	echo_log "P$process_id completed downloading $table"
}

# Receves $num_chunks $current_chunk
calculate_chunk_of() {
 	local num_chunks=$1
	local i=$2
	local chunk_of="$((i)) / $num_chunks"   
    echo "$chunk_of"
}
#
# This function downloads a table from the Iris production server
# and saves it in a local file.
#
download_prod_table_chunk() {
	local process_id=$1
	local table=$2
	local chunk_num=$3
	local num_chunks=$4
	local chunk_of=$5
	local offset=$((chunk_num * CHUNK_SIZE))
	local local_file

	mkdir -p "${LOCAL_DIR}"
	local_file="${LOCAL_DIR}/${table}.${offset}.parquet"
	if [[ -f "${local_file}" ]]; then
        echo_log "P$process_id already downloaded chunk $chunk_of of $table"
		return 0
	fi
	if ! curl -s -X POST -o "${local_file}" \
		--user "${PROD_CH_USER}:${PROD_CH_PASSWD}" \
		-d "SELECT * FROM ${PROD_CH_DB}.${table} LIMIT ${CHUNK_SIZE} OFFSET ${offset} FORMAT Parquet" \
		"${PROD_CH_URL}"; then
		echo_log "P$process_id failed to download chunk $chunk_of of $table"
		return 1
	fi

	echo_log "P$process_id finished downloading chunk $chunk_of of $table"
}

#
# This function first creates a table and then uploads results into
# to.
#
# XXX Add code to skip creating the table if it already exists.
# XXX Fix the query so it would work running directly on the dev server.
#
upload_dev_table() {
	local process_id="$1"
	local table="$2"
	local num_chunks=$3
	local chunk_of
	local create_sql
	local load_sql
    local i

	case "${table}" in
	cleaned_links__*) create_sql=$(links_sql "${table}");;
	cleaned_prefixes__*) create_sql=$(prefixes_sql "${table}");;
	cleaned_probes__*) create_sql=$(probes_sql "${table}");;
	*results__*) create_sql=$(results_sql "${table}");;
	*) echo "${table}: unknown table"; return 1;;
	esac
    i=0
	if [[ -z "${DEV_SSH_USER+x}" ]]; then
        if ! clickhouse client --user "${DEV_CH_USER}" --password "${DEV_CH_PASSWD}" -q "${create_sql//\\/}" 2>/dev/null; then
            echo_log "P$process_id table already exists for $table skipping"
            return
        fi

		load_sql="INSERT INTO ${DEV_CH_DB}.${table} FORMAT Parquet"
		for t in "${LOCAL_DIR}/${table}"*; do
            chunk_of="$((i)) / $num_chunks"
			clickhouse client --user "${DEV_CH_USER}" --password "${DEV_CH_PASSWD}" -q "${load_sql}" < "${t}"
            echo_log "P$process_id finished uploading chunk ${chunk_of} of $table"
            i=$((i + 1))
		done
	else
		if ! ssh "${DEV_SSH_USER}@${IRIS_DEV_HOSTNAME}" clickhouse-client --user "${DEV_CH_USER}" --password "${DEV_CH_PASSWD}" -q \""${create_sql}"\" 2>/dev/null; then
            echo_log "P$process_id table already exists for $table skipping"
            return
        fi
		load_sql="INSERT INTO ${DEV_CH_DB}.${table} FORMAT Parquet"
		for t in "${LOCAL_DIR}/${table}"*; do
            chunk_of="$((i)) / $num_chunks"
			ssh "${DEV_SSH_USER}@${IRIS_DEV_HOSTNAME}" clickhouse-client --user "${DEV_CH_USER}" --password "${DEV_CH_PASSWD}" -q \""${load_sql}"\" < "${t}"
            echo_log "P$process_id finished uploading chunk ${chunk_of} of $table"
            i=$((i + 1))
		done
	fi

}

# Logs the progress with the date and time specified.
echo_log() {
	# datetime_format="+%Y-%m-%d %H:%M:%S"
	# echo "[$(date -j -f '%s' "$(date +%s)" "$datetime_format")]:" $@
	# shellcheck disable=SC2046
	echo $(date -u) ":" "$@"
}

links_sql() {
	local table="$1"

	cat <<EOF
CREATE TABLE ${DEV_CH_DB}.$table (     \`probe_protocol\` UInt8,     \`probe_src_addr\` IPv6,     \`probe_dst_prefix\` IPv6,     \`probe_dst_addr\` IPv6,     \`probe_src_port\` UInt16,     \`probe_dst_port\` UInt16,     \`near_round\` UInt8,     \`far_round\` UInt8,     \`near_ttl\` UInt8,     \`far_ttl\` UInt8,     \`near_addr\` IPv6,     \`far_addr\` IPv6 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port) SETTINGS index_granularity = 8192
EOF
}

prefixes_sql() {
	local table="$1"

	cat <<EOF
CREATE TABLE ${DEV_CH_DB}.$table (     \`probe_protocol\` UInt8,     \`probe_src_addr\` IPv6,     \`probe_dst_prefix\` IPv6,     \`has_amplification\` UInt8,     \`has_loops\` UInt8 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix) SETTINGS index_granularity = 8192
EOF
}

probes_sql() {
	local table="$1"

	cat <<EOF
CREATE TABLE ${DEV_CH_DB}.$table (     \`probe_protocol\` UInt8,     \`probe_dst_prefix\` IPv6,     \`probe_ttl\` UInt8,     \`cumulative_probes\` UInt32,     \`round\` UInt8 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_dst_prefix, probe_ttl) SETTINGS index_granularity = 8192
EOF
}

# 
# Note that after the data XXX (find the date) the measurements are no longer using the following 
# column names (no other column is added). So the following columns are commented. If you want to 
# get a table which is older than that date, then uncomment the lines in the creation script.
# If you already created those, you can simply ignore them.
#    'destination_host_reply',
#    'private_probe_dst_prefix',
#    'destination_prefix_reply',
#    'probe_dst_prefix',
#    'time_exceeded_reply',
#    'reply_src_prefix',
#    'valid_probe_protocol',
#    'private_reply_src_addr'
#
results_sql() {
	local table="$1"

	cat <<EOF
CREATE TABLE ${DEV_CH_DB}.$table (
    \\\`capture_timestamp\\\` DateTime,
    \\\`probe_protocol\\\` UInt8,
    \\\`probe_src_addr\\\` IPv6,
    \\\`probe_dst_addr\\\` IPv6,
    \\\`probe_src_port\\\` UInt16,
    \\\`probe_dst_port\\\` UInt16,
    \\\`probe_ttl\\\` UInt8,
    \\\`quoted_ttl\\\` UInt8,
    \\\`reply_src_addr\\\` IPv6,
    \\\`reply_protocol\\\` UInt8,
    \\\`reply_icmp_type\\\` UInt8,
    \\\`reply_icmp_code\\\` UInt8,
    \\\`reply_ttl\\\` UInt8,
    \\\`reply_size\\\` UInt16,
    \\\`reply_mpls_labels\\\` Array(Tuple(UInt32, UInt8, UInt8, UInt8)),
    \\\`rtt\\\` UInt16,
    \\\`round\\\` UInt8,
    -- \\\`probe_dst_prefix\\\` IPv6,
    -- \\\`reply_src_prefix\\\` IPv6,
    -- \\\`private_probe_dst_prefix\\\` UInt8,
    -- \\\`private_reply_src_addr\\\` UInt8,
    -- \\\`destination_host_reply\\\` UInt8,
    -- \\\`destination_prefix_reply\\\` UInt8,
    -- \\\`valid_probe_protocol\\\` UInt8,
    -- \\\`time_exceeded_reply\\\` UInt8
)
ENGINE = MergeTree
ORDER BY (
    probe_protocol,
    probe_src_addr,
    -- probe_dst_prefix,
    probe_dst_addr,
    probe_src_port,
    probe_dst_port,
    probe_ttl
)
SETTINGS index_granularity = 8192
EOF
}

main "$@"
