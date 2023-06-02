package writer

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"columndb/models"
	"columndb/utils"
)

func (w *Writer) getNextIndex() int {
	w.currentIndex++
	return w.currentIndex
}

func (w *Writer) Setup() error {
	indexPath := w.dataDir + IndexFile
	file, err := os.OpenFile(indexPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrapf(err, "failed to open index file %s", indexPath)
	}

	stat, _ := file.Stat()
	filesize := stat.Size()

	currentIndex := 0
	if filesize > 0 {
		lastLine, err := getLastLine(file)
		if err != nil {
			return errors.Wrapf(err, "failed to get last line from index file %s", indexPath)
		}
		currentIndex = lastLine.Index
	}

	w.currentIndex = currentIndex
	w.fileHandles = map[string]*os.File{
		IndexFile: file,
	}

	return err
}

func getLastLine(file *os.File) (ValueRow, error) {
	lastLine, err := utils.GetLastLineBytes(file)
	if err != nil {
		return ValueRow{}, errors.Wrapf(err, "failed to get last line from file %s", file.Name())
	}

	vals := strings.SplitN(string(lastLine), ",", 2)
	index, err := strconv.Atoi(vals[0])
	if err != nil {
		return ValueRow{}, errors.Wrapf(err, "failed to convert index %s to int", vals[0])
	}
	value, err := strconv.Atoi(vals[1])
	if err != nil {
		return ValueRow{}, errors.Wrapf(err, "failed to convert value %s to int", vals[1])
	}
	row := ValueRow{Index: index, Value: value}

	return row, nil
}

func (w *Writer) SaveEvent(e models.Event) error {
	w.m.Lock()
	defer w.m.Unlock()

	index := w.getNextIndex()

	indexString := fmt.Sprintf("%d,%d\n", index, w.clock.Now().Unix())
	indexFile := w.fileHandles[IndexFile]
	if _, err := indexFile.WriteString(indexString); err != nil {
		return errors.Wrapf(err, "failed to write index to file %s", indexFile.Name())
	}

	for fieldName, fieldValue := range e.Fields {
		extension := guessType(fieldValue)
		filePath := w.dataDir + fieldName + "." + extension

		_, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				_, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
				if err != nil {
					return errors.Wrapf(err, "failed to open file %s", filePath)
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
	case bool:
		return "bool"
	default:
		return "string"
	}
}
