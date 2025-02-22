build:
	go build -o mpat cmd/main.go
copy_iris_tables:
	xargs -I {} ./mpat util copyiristables {} --force-truncate < ./data/meas_uuids.txt
