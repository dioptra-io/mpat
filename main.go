package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/retina"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	client := retina.NewRetinaClient(retina.Config{})

	for r := range client.Stream(ctx) {
		if r.Err != nil {
			log.Fatal(r.Err)
		}
		for _, fie := range r.Batch {
			fmt.Println(fie.SequenceNumber, fie.DestinationAddress)
		}
	}
}
