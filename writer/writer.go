package writer

import (
	"fmt"
	"os"
	"sync"

	"columndb/models"

	"github.com/gocarina/gocsv"
	"github.com/jonboulle/clockwork"
)

type Writer struct {
	m     sync.Mutex
	clock clockwork.Clock

	dataDir      string
	currentIndex int

	fileHandles map[string]*os.File
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

	rowString := fmt.Sprintf("%d,%d\n", index, w.clock.Now().Unix())

	indexFile := w.fileHandles["index.int"]

	if _, err := indexFile.WriteString(rowString); err != nil {
		return err
	}

	return nil
}

type Row struct {
	Index     int `csv:"index"`
	Timestamp int `csv:"timestamp"`
}

func (w *Writer) getNextIndex() int {
	w.currentIndex++
	return w.currentIndex
}
