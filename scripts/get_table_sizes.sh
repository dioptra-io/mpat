#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

main () {
    local input
	local i
    local count_sql
    local num_rows
    local prod
    local user="${DEV_CH_USER}"
    local passwd="${DEV_CH_PASSWD}"
    local db="${DEV_CH_DB}"
    local url="${DEV_CH_URL}"

	if [[ $# -ne 2 ]]; then
		echo "$0: missing argument"
		return 1
	fi
    prod="$1"
	input="$2"

    if [[ $prod -eq "1" ]]; then
        user="${PROD_CH_USER}"
        passwd="${PROD_CH_PASSWD}"
        db="${PROD_CH_DB}"
        url="${PROD_CH_URL}"
    fi

	i=0
	while read -r table; do
        count_sql="SELECT count(*) FROM ${db}.${table} FORMAT CSV;"
        
        if [[ $prod -eq "0" ]]; then 
            num_rows=$(clickhouse client --user "${user}" --password "${passwd}" -q "${count_sql}" | tr -d '"')
        else 
            num_rows=$(curl -s -X POST --user "${user}:${passwd}" "${url}" -d "${count_sql}" | tr -d '"')
        fi 
        # echo_log "${table} has ${num_rows} row(s)"
        echo "${prod}" "${table}" "${num_rows}"

		i=$((i + 1))
	done < "$input"
}

main "$@"
