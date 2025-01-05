package nexthop

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"dioptra-io/ufuk-research/internal"
)

var (
	fNumWorkers  int
	fProgressBar bool
)

var CountNextHopsCmd = &cobra.Command{
	Use:   "cnh",
	Short: "This command counts the number of next hops in the given table",
	Long:  "This is a program that consumes the data in the clikhouse database and computes a result.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Println("Missing argument <table_name>")
			return
		}

		tableName := args[0]
		// fmt.Printf("Running workers (%d of them to be exact)\n", fNumWorkers)
		// fmt.Printf("resultTableName: %v\n", resultTableName)

		conn, err := internal.NewConnection()
		if err != nil {
			fmt.Printf("Cannot connect to the clickhouse database %s\n", err)
			return
		}

		if err := runWorker(tableName, fNumWorkers, conn); err != nil {
			fmt.Printf("An error occured while running the workers%s\n", err)
			return
		}
	},
}

func init() {
	// Define persistent flags
	CountNextHopsCmd.PersistentFlags().
		IntVarP(&fNumWorkers, "num-workers", "w", 32, "This is the number of workers for processing the next hop count")
	CountNextHopsCmd.PersistentFlags().
		BoolVar(&fProgressBar, "progress", false, "Set this flag to true for the progressbar")
}

func query(tableName string) (string, string) {
	countQuery := `
WITH 
	toIPv6(cutIPv6(probe_dst_addr, 0, 1)) AS probe_dst_pf
SELECT count(DISTINCT 
		probe_src_addr, 
		probe_dst_pf, 
		probe_dst_addr, 
		probe_src_port)
FROM %s
`

	selectQuery := `
WITH 
    toIPv6(cutIPv6(probe_dst_addr, 0, 1)) AS probe_dst_pf,
    groupArray((probe_ttl, reply_src_addr)) AS reply_and_ttl_array,
    arraySort((x) -> x.1, reply_and_ttl_array) AS sorted_elems,
    arrayMap(x -> x.1, sorted_elems) AS probe_ttl_array,
    arrayMap(x -> x.2, sorted_elems) AS reply_src_array
SELECT
    -- probe_src_addr,
    -- probe_dst_addr,
    -- probe_src_port,
    -- probe_ttl_array,
    -- reply_src_array
    probe_ttl_array
FROM %s
GROUP BY
    probe_src_addr,
    probe_dst_pf,
    probe_dst_addr,
    probe_src_port
ORDER BY
    probe_src_addr ASC,
    probe_dst_pf ASC,
    probe_dst_addr ASC,
    probe_src_port ASC;
`

	return fmt.Sprintf(countQuery, tableName), fmt.Sprintf(selectQuery, tableName)
}

func runWorker(tableName string, numWorkers int, conn clickhouse.Conn) error {
	var numRows uint64
	var ttls []uint8
	var pb *progressbar.ProgressBar
	totalNumberOfNextHops := 0
	countQuery, selectQuery := query(tableName)

	if fProgressBar {
		if err := conn.QueryRow(context.TODO(), countQuery).Scan(&numRows); err != nil {
			return err
		}
		pb = progressbar.Default(int64(numRows), "Calculating Number of Next Hops")
	}

	rows, err := conn.Query(context.TODO(), selectQuery)
	if err != nil {
		return err
	}

	// First do without the parallel stuff
	for rows.Next() {
		if err := rows.Scan(&ttls); err != nil {
			return err
		}
		totalNumberOfNextHops += numConsecutives(ttls)

		if pb != nil {
			pb.Add(1)
		}
	}

	fmt.Printf("%v\n", totalNumberOfNextHops)
	return nil
}

func numConsecutives(array []uint8) int {
	numCons := 0

	for i := 0; i < len(array)-1; i += 1 {
		if array[i]+1 == array[i+1] {
			numCons += 1
		}
	}
	return numCons
}

// XXX These functions and definitions will be used later when we want to do more specialized analysis
// for now let them stay here, in the furure I will transfer them to commons.
// // This case the reduce function finds the number of successive elements since the results Row
// // columns are identical.
// func reduceFunction(res *resultRow) (int, error) {
// 	numSuccessorts := 0
//
// 	for i := 0; i < len(res.ProbeTTLArray); i++ {
// 		if res.ProbeTTLArray[i]+1 == res.ProbeTTLArray[i+1] {
// 			numSuccessorts += 1
// 		}
// 	}
//
// 	return numSuccessorts, nil
// }
//
// type resultRow struct {
// 	// This is the list of probe ttls, ordered from low to high
// 	ProbeTTLArray []uint8
//
// 	// This is the source addresses
// 	ReplyAddressArray []net.IP
//
// 	// This is the destination address of the probe
// 	ProbeDestinationAddress net.IP
//
// 	// The original source address
// 	ProbeSourceAddress net.IP
//
// 	// The source port of the probe
// 	ProbeSourcePort uint16
// }
//
// func Scan(rows driver.Rows) (*resultRow, error) {
// 	res := &resultRow{}
// 	if err := rows.Scan(&res.ProbeSourceAddress, &res.ProbeDestinationAddress, &res.ProbeSourcePort, &res.ReplyAddressArray, &res.ProbeTTLArray); err != nil {
// 		return nil, err
// 	}
// 	return res, nil
// }
