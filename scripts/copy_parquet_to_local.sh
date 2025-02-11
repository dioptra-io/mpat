#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

main () {
    local route_table_name
    local file
    local query
    
    for file in routes/*.csv; do
        route_table_name="${file/_results/}"
        route_table_name="${route_table_name/routes\//}"
        route_table_name="${route_table_name/.csv/}"
        query="INSERT INTO iris.${route_table_name} FORMAT CSV"
        clickhouse-client --query "${query}" < "$file"
        echo "${file} done!"
        # echo "${query} done!"
    done
}

main "$@"

