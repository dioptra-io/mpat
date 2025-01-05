# Research Material and Code
1. To download the tables from the prod and upload to dev, you need the .env file. After compiling the ,env file you can use the `download_tables.sh` script.
2. Afterwards, you can compile the `consumerctl` using `go build -o consumerctl cmd/main.go`.
3. Then you can use the `calculate_num_next_hops.sh` script to calculate number of next hop information from the guven tables.
