package api

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Represents a node and a verison. It is written as <node-name>/v<node-version>. For example:
// "ingestion_node/v2".
type NamedVersion struct {
	Name    string `json:"name"`
	Version uint   `json:"version"`
}

func NewNV(name string, version uint) NamedVersion {
	return NamedVersion{
		Name:    name,
		Version: version,
	}
}

// Value implements the driver.Valuer interface for GORM
func (nv NamedVersion) Value() (driver.Value, error) {
	return json.Marshal(nv)
}

// Scan implements the sql.Scanner interface for GORM
func (nv *NamedVersion) Scan(value any) error {
	if value == nil {
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("failed to scan NamedVersion: unsupported type %T", value)
	}

	return json.Unmarshal(bytes, nv)
}

func (nv NamedVersion) String() string {
	return fmt.Sprintf("%s/v%d", nv.Name, nv.Version)
}
