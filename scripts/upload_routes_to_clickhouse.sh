#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

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

    for file in "routes"/*; do 
        if [[ -f "$file" ]]; then
            clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --query="INSERT INTO $CLICKHOUSE_DATABASE.$CLICKHOUSE_TABLE FORMAT Parquet" < "$file"
            
            if [[ $? -eq 0 ]]; then
                echo "Successfully uploaded $file"
            else
                echo "Failed to upload $file" >&2
            fi
        fi
    done
}

main "$@"
