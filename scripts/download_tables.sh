#!/bin/bash
# set -x 

source ../.env
# Variables
# DEV_CH_USER=""
# DEV_CH_PASSWD=""
# DEV_CH_DB=""
# DEV_CH_URL=""
# PROD_CH_USER=""
# PROD_CH_PASSWD=""
# PROD_CH_DB=""
# PROD_CH_URL=""

# One million rows ()
CHUNK_SIZE=10000000

# Logs the progress with the date and time specified.
echo_log() {
    # datetime_format="+%Y-%m-%d %H:%M:%S"
    # echo "[$(date -j -f '%s' "$(date +%s)" "$datetime_format")]:" $@
    echo $(date -u) ":" $@
}

# download_table_chunk $table $i $num_chunks $process_id
download_table_chunk() {
    database="iris"
    datetime_format="+%Y-%m-%d %H:%M:%S"
    table=$1
    i=$2 
    num_chunks=$3
    process_id=$4

    offset=$((i * CHUNK_SIZE))
    offset_str="$((i+1))/$num_chunks"

    # no piping save to a file
    curl -X POST -s --user "$PROD_CH_USER:$PROD_CH_PASSWD" -o "data/$table.parquet" -d "SELECT * FROM $database.$table FORMAT Parquet" "$PROD_CH_URL" 
    

    #  ./clickhouse client -q "INSERT INTO ${database}.${table} FORMAT Native"
    download_result=$?

    # Check for fails
    if [[ $download_result -ne 0 ]]; then
        echo_log "P$process_id failed on download offset $offset_str for $table"
        continue
    fi

    echo_log "P$process_id finished download for $table"
}

download_table() {
    process_id=$1
    table=$2
    database="iris"
    datetime_format="+%Y-%m-%d %H:%M:%S"

    if [[ $table == cleaned_links* ]]; then 
        create_sql="CREATE TABLE $database.$table (     \`probe_protocol\` UInt8,     \`probe_src_addr\` IPv6,     \`probe_dst_prefix\` IPv6,     \`probe_dst_addr\` IPv6,     \`probe_src_port\` UInt16,     \`probe_dst_port\` UInt16,     \`near_round\` UInt8,     \`far_round\` UInt8,     \`near_ttl\` UInt8,     \`far_ttl\` UInt8,     \`near_addr\` IPv6,     \`far_addr\` IPv6 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port) SETTINGS index_granularity = 8192"
    elif [[ $table == cleaned_prefixes* ]]; then
        create_sql="CREATE TABLE $database.$table (     \`probe_protocol\` UInt8,     \`probe_src_addr\` IPv6,     \`probe_dst_prefix\` IPv6,     \`has_amplification\` UInt8,     \`has_loops\` UInt8 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix) SETTINGS index_granularity = 8192"
    elif [[ $table == cleaned_probes* ]]; then
        create_sql="CREATE TABLE $database.$table (     \`probe_protocol\` UInt8,     \`probe_dst_prefix\` IPv6,     \`probe_ttl\` UInt8,     \`cumulative_probes\` UInt32,     \`round\` UInt8 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_dst_prefix, probe_ttl) SETTINGS index_granularity = 8192"
    elif [[ $table == cleaned_results* ]]; then
        create_sql="CREATE TABLE $database.$table (     \`capture_timestamp\` DateTime,     \`probe_protocol\` UInt8,     \`probe_src_addr\` IPv6,     \`probe_dst_addr\` IPv6,     \`probe_src_port\` UInt16,     \`probe_dst_port\` UInt16,     \`probe_ttl\` UInt8,     \`quoted_ttl\` UInt8,     \`reply_src_addr\` IPv6,     \`reply_protocol\` UInt8,     \`reply_icmp_type\` UInt8,     \`reply_icmp_code\` UInt8,     \`reply_ttl\` UInt8,     \`reply_size\` UInt16,     \`reply_mpls_labels\` Array(Tuple(UInt32, UInt8, UInt8, UInt8)),     \`rtt\` UInt16,     \`round\` UInt8,     \`probe_dst_prefix\` IPv6,     \`reply_src_prefix\` IPv6,     \`private_probe_dst_prefix\` UInt8,     \`private_reply_src_addr\` UInt8,     \`destination_host_reply\` UInt8,     \`destination_prefix_reply\` UInt8,     \`valid_probe_protocol\` UInt8,     \`time_exceeded_reply\` UInt8 ) ENGINE = MergeTree ORDER BY (probe_protocol, probe_src_addr, probe_dst_prefix, probe_dst_addr, probe_src_port, probe_dst_port, probe_ttl) SETTINGS index_granularity = 8192"
    else
        echo "Unkown table type"
        continue
    fi

    mkdir -p temp

    # Create the table
    # ./clickhouse client -q "$create_sql" 2> /dev/null || true

    # Truncate file
    # ./clickhouse client -q "TRUNCATE TABLE $database.$table" || true

    # Get number of chunks
    num_rows=$(curl -X POST -s --user "$PROD_CH_USER:$PROD_CH_PASSWD" -d "SELECT count(*) FROM $database.$table FORMAT CSV" "$PROD_CH_URL" --output -)
    num_chunks=$((num_rows / CHUNK_SIZE))
    num_chunks_processes=20

    # start log
    # echo_log "P$process_id starting for table $table"
    echo_log "P$process_id  starting download for $table"
    # for i in $(seq 0 1 $num_chunks); do
    download_table_chunk $table $i $num_chunks $process_id
    # done

    # end log
    # echo_log "P$process_id completed table $table"
}

i=0
for table in $(cat $1); do
    download_table $i $table
    i=$((i + 1))
done

wait 

echo_log "Finished"

