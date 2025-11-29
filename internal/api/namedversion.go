package api

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

// Represents a node and a verison. It is written as <node-name>/v<node-version>. For example: "ingestion_node/v2".
type NamedVersion struct {
	Name    string
	Version uint
}

func NewNV(name string, version uint) NamedVersion {
	return NamedVersion{Name: name, Version: version}
}

func (nv NamedVersion) String() string {
	return fmt.Sprintf("%s/v%d", nv.Name, nv.Version)
}

// Database seralization.

func (nv NamedVersion) Value() (driver.Value, error) {
	return nv.String(), nil
}

func (nv *NamedVersion) Scan(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("NamedVersion.Scan: expected string, got %T", value)
	}

	// String format: "<name>/v<version>"
	parts := strings.Split(str, "/v")
	if len(parts) != 2 {
		return fmt.Errorf("NamedVersion.Scan: invalid format '%s'", str)
	}

	version, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("NamedVersion.Scan: invalid version '%s'", parts[1])
	}

	nv.Name = parts[0]
	nv.Version = uint(version)
	return nil
}
