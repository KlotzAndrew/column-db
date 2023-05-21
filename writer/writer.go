package writer

import (
	"fmt"
	"os"
	"sync"
	"time"

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
	indexPath := w.dataDir + "index.int"
	f, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		if _, err := f.WriteString("index,timestamp\n"); err != nil {
			return err
		}
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

	rowString := fmt.Sprintf("%d,%d\n", index, time.Now().Unix())

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
