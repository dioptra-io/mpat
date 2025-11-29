package api

import (
	"fmt"
	"strconv"
	"strings"
)

// Represents a node and a verison. It is written as <node-name>/v<node-version>. For example: "ingestion_node/v2".
type NamedVersion string

func NewNV(name string, version uint) NamedVersion {
	return NamedVersion(fmt.Sprintf("%s/v%d", name, version))
}

func (nv NamedVersion) Name() string {
	parts := strings.Split(string(nv), "/v")
	return parts[0]
}

func (nv NamedVersion) Version() uint {
	parts := strings.Split(string(nv), "/v")
	v, _ := strconv.Atoi(parts[1])
	return uint(v)
}

func (nv NamedVersion) String() string {
	return string(nv)
}
