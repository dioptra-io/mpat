package retina

import api "github.com/dioptra-io/retina-commons/api/v1"

// SequencedFIE is a ForwardingInfoElement with a sequence number for ordered
// delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}
