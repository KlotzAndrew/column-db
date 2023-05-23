package writer

import (
	"fmt"
	"os"
	"sync"

	"columndb/models"

	"github.com/gocarina/gocsv"
	"github.com/jonboulle/clockwork"
	"github.com/pkg/errors"
)

type Writer struct {
	m     sync.Mutex
	clock clockwork.Clock

	dataDir string

	currentIndex int
	fileHandles  map[string]*os.File
}

func NewWriter(dataDir string, clock clockwork.Clock) Writer {
	return Writer{
		dataDir: dataDir,
		clock:   clock,
	}
}

func (w *Writer) Setup() error {
	indexPath := w.dataDir + "index.int"
	f, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	rows := []*Row{}
	if err := gocsv.UnmarshalFile(f, &rows); err != nil {
		if err == gocsv.ErrEmptyCSVFile {
			if _, err := f.WriteString("index,timestamp\n"); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	w.currentIndex = len(rows)

	w.fileHandles = map[string]*os.File{
		"index.int": f,
	}

	return err
}

func (w *Writer) GetEvent(id int) (models.Event, error) {
	indexPath := w.dataDir + "index.int"
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return models.Event{}, err
	}

	rows := []*Row{}
	if err := gocsv.UnmarshalFile(indexFile, &rows); err != nil {
		return models.Event{}, err
	}

	row := rows[id-1]

	return models.Event{
		ID:        row.Index,
		Timestamp: row.Timestamp,
	}, nil
}

func (w *Writer) SaveEvent(e models.Event) error {
	w.m.Lock()
	defer w.m.Unlock()

	index := w.getNextIndex()

	indexString := fmt.Sprintf("%d,%d\n", index, w.clock.Now().Unix())
	indexFile := w.fileHandles["index.int"]
	if _, err := indexFile.WriteString(indexString); err != nil {
		return errors.Wrapf(err, "failed to write index to file %s", indexFile.Name())
	}

	for fieldName, fieldValue := range e.Fields {
		extension := guessType(fieldValue)
		filePath := w.dataDir + fieldName + "." + extension

		_, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
				if err != nil {
					return errors.Wrapf(err, "failed to open file %s", filePath)
				}
				headerRow := fmt.Sprintf("index,value\n")
				if _, err := file.WriteString(headerRow); err != nil {
					return errors.Wrapf(err, "failed to write header row to file %s", filePath)
				}
			}
		}

		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			return errors.Wrapf(err, "failed to open file %s", filePath)
		}

		rowString := fmt.Sprintf("%d,%v\n", index, fieldValue)
		if _, err := file.WriteString(rowString); err != nil {
			return errors.Wrapf(err, "failed to write row to file %s, %s", filePath, rowString)
		}
	}

	return nil
}

func guessType(value any) string {
	switch value.(type) {
	case float64:
		return "float"
	case int:
		return "int"
	case string:
		return "string"
	default:
		return "string"
	}
}

type Row struct {
	Index     int `csv:"index"`
	Timestamp int `csv:"timestamp"`
}

func (w *Writer) getNextIndex() int {
	w.currentIndex++
	return w.currentIndex
}
