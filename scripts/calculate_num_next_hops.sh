#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

main () {
    local input
	local i
    local cnh_table
    local total_cnh=0

	if [[ $# -ne 1 ]]; then
		echo "$0: missing argument"
		return 1
	fi
	input="$1"

	i=0
	while read -r table; do
        # echo_log "$i" "$table"
        echo_log "Starting next hop analysis of $table"

        cnh_table=$(./consumerctl cnh "$table" -w 10 -p "$DEV_CH_PASSWD" -u "$DEV_CH_USER")
        total_cnh=$((total_cnh + cnh_table))

        echo_log "Finished next hop analysis of ${table} (current: ${cnh_table}, total: ${total_cnh})"

		i=$((i + 1))
	done < "$input"


    echo_log "Total of ${total_cnh} next hop information is calculated from the given tables"
}

# Logs the progress with the date and time specified.
echo_log() {
	echo "$(date -u)" ":" "$@"
}

main "$@"
