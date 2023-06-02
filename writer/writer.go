package writer

import (
	"os"
	"sync"

	"github.com/jonboulle/clockwork"
)

type Writer struct {
	m     sync.Mutex
	clock clockwork.Clock

	dataDir string

	currentIndex int
	fileHandles  map[string]*os.File
}

type Row struct {
	Index     int
	Timestamp int
}

type ValueRow struct {
	Index int
	Value any
}

func NewWriter(dataDir string, clock clockwork.Clock) *Writer {
	return &Writer{
		dataDir: dataDir,
		clock:   clock,
	}
}

func convertToType(value any, valueType string) any {
	switch valueType {
	case "float":
		return value.(float64)
	case "int":
		return value.(int)
	case "string":
		return value.(string)
	default:
		return value.(string)
	}
}

func stringToType(value any) any {
	switch value.(type) {
	case float64:
		return value.(float64)
	case int:
		return value.(int)
	case string:
		return value.(string)
	default:
		return value.(string)
	}
}
