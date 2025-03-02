build:
	go build -o mpat cmd/main.go
copy_iris_tables:
	xargs -I {} ./mpat util copyiristables {} --force-truncate < ./data/meas_uuids.txt
copy_ark_tables:
	./mpat copy arkdata '2025-02-01' '2025-02-11' --force-delete
