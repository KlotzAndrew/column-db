package writer

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"columndb/models"
	"columndb/utils"

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

func NewWriter(dataDir string, clock clockwork.Clock) *Writer {
	return &Writer{
		dataDir: dataDir,
		clock:   clock,
	}
}

func (w *Writer) Setup() error {
	indexPath := w.dataDir + "index.int"
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
		"index.int": file,
	}

	return err
}

func getLastLine(file *os.File) (ValueRow, error) {
	lastLine, err := utils.GetLastLine(file)
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

func (w *Writer) Avg(fieldName string) (float64, error) {
	files, err := ioutil.ReadDir(w.dataDir)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read data directory %s", w.dataDir)
	}

	var sum float64
	var count float64
	for _, file := range files {
		if file.Name() == "index.int" || file.Name() == ".keep" {
			continue
		}

		// check if filename starts with fieldname
		if !strings.HasPrefix(file.Name(), fieldName) {
			continue
		}

		file, err := os.Open(w.dataDir + file.Name())
		if err != nil {
			return 0, errors.Wrapf(err, "failed to open file %s", file.Name())
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			_, suffix := filePrefixSuffix(file.Name())
			if suffix == "int" {
				vals := strings.SplitN(scanner.Text(), ",", 2)
				value, err := strconv.Atoi(vals[1])
				if err != nil {
					return 0, errors.Wrapf(err, "failed to convert value %s to int", vals[1])
				}
				sum += float64(value)
				count++
			} else if suffix == "float" {
				vals := strings.SplitN(scanner.Text(), ",", 2)
				value, err := strconv.ParseFloat(vals[1], 64)
				if err != nil {
					return 0, errors.Wrapf(err, "failed to convert value %s to float", vals[1])
				}
				sum += value
				count++
			} else {
				panic("unknown type")
			}
		}

		break
	}

	res := float64(sum) / float64(count)

	return res, nil
}

func filePrefixSuffix(fileName string) (string, string) {
	fieldName := strings.Split(fileName, ".")[0]
	fieldType := strings.Split(fileName, ".")[1]
	return fieldName, fieldType
}

func (w *Writer) GetEvent(id int) (models.Event, error) {
	indexPath := w.dataDir + "index.int"
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return models.Event{}, errors.Wrapf(err, "failed to open index file %s", indexPath)
	}

	rows := []Row{}
	scanner := bufio.NewScanner(indexFile)
	for scanner.Scan() {
		vals := strings.SplitN(scanner.Text(), ",", 2)
		index, err := strconv.Atoi(vals[0])
		if err != nil {
			return models.Event{}, errors.Wrapf(err, "failed to convert index %s to int", vals[0])
		}
		timestamp, err := strconv.Atoi(vals[1])
		if err != nil {
			return models.Event{}, errors.Wrapf(err, "failed to convert timestamp %s to int", vals[1])
		}
		row := Row{Index: index, Timestamp: timestamp}
		rows = append(rows, row)
	}

	row := rows[id-1]
	event := models.Event{
		ID:        row.Index,
		Timestamp: row.Timestamp,
		Fields:    map[string]any{},
	}

	files, err := ioutil.ReadDir(w.dataDir)
	if err != nil {
		return models.Event{}, errors.Wrapf(err, "failed to read data directory %s", w.dataDir)
	}
	for _, file := range files {
		if file.Name() == "index.int" || file.Name() == ".keep" {
			continue
		}
		file, err := os.Open(w.dataDir + file.Name())
		if err != nil {
			return models.Event{}, errors.Wrapf(err, "failed to open file %s", file.Name())
		}

		row := ValueRow{}
		found := false
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			vals := strings.SplitN(scanner.Text(), ",", 2)
			rowId, err := strconv.Atoi(vals[0])
			if err != nil {
				return models.Event{}, err
			}

			if rowId == id {
				row = ValueRow{Index: rowId, Value: vals[1]}
				found = true
			} else if rowId > id {
				break
			}
		}

		if !found {
			continue
		}

		fileName := filepath.Base(file.Name())
		fieldName := strings.Split(fileName, ".")[0]
		fieldType := strings.Split(fileName, ".")[1]

		switch fieldType {
		case "int":
			v, ok := row.Value.(int)
			if !ok {
				return models.Event{}, errors.Errorf("failed to convert value %s to int", row.Value)
			}
			row.Value = v
		case "float":
			s, ok := row.Value.(string)
			if !ok {
				return models.Event{}, errors.Errorf("failed to convert value %s to string", row.Value)
			}

			parsed, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return models.Event{}, errors.Wrapf(err, "failed to convert value %s to float", row.Value)
			}
			row.Value = parsed
		case "string":
			v, ok := row.Value.(string)
			if !ok {
				return models.Event{}, errors.Errorf("failed to convert value %s to string", row.Value)
			}
			row.Value = v
		case "bool":
			v, ok := row.Value.(string)
			if !ok {
				return models.Event{}, errors.Errorf("failed to convert value %s to string", row.Value)
			}
			value, err := strconv.ParseBool(v)
			if err != nil {
				return models.Event{}, errors.Wrapf(err, "failed to convert value %s to bool", row.Value)
			}

			row.Value = value
		default:
			panic("unknown type")
		}

		event.Fields[fieldName] = row.Value
	}

	return event, nil
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

type Row struct {
	Index     int
	Timestamp int
}

type ValueRow struct {
	Index int
	Value any
}

func (w *Writer) getNextIndex() int {
	w.currentIndex++
	return w.currentIndex
}
