package writer

import (
	"fmt"
	"os"
	"sync"

	"github.com/gocarina/gocsv"

	"columndb/models"
)

type Writer struct {
	m sync.Mutex

	dataDir string

	fileHandles map[string]*os.File
}

func NewWriter(dataDir string) Writer {
	return Writer{
		dataDir: dataDir,
	}
}

func (w *Writer) Setup() error {
	f, err := os.OpenFile(w.dataDir+"index.int", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	w.fileHandles = map[string]*os.File{
		"index.int": f,
	}

	return nil
}

func (w *Writer) SaveEvent(e models.Event) error {
	index, err := w.getNextIndex()
	if err != nil {
		return err
	}

	fmt.Println(index)

	return nil
}

type Row struct {
	Index     int
	Timestamp int
}

// read a file line by line and convert it to a Row struct

func (w *Writer) getNextIndex() (int, error) {
	w.m.Lock()
	defer w.m.Unlock()

	indexFile := w.fileHandles["index.int"]

	rows := []*Row{}

	if err := gocsv.UnmarshalFile(indexFile, &rows); err != nil {
		if err == gocsv.ErrEmptyCSVFile {
			return 0, nil
		}
		panic(err)
	}

	lastRow := rows[len(rows)-1]
	return lastRow.Index + 1, nil
}
