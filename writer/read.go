package writer

import (
	"bufio"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"columndb/models"

	"github.com/pkg/errors"
)

const (
	Equal            = "="
	LessThan         = "<"
	GreaterThanEqual = ">="
	GreaterThan      = ">"
	LessThanEqual    = "<="
)

const IndexFile = "index.int"

type Query struct {
	Filters map[string][]string
}

func (w *Writer) Avg(fieldName string) (float64, error) {
	files, err := ioutil.ReadDir(w.dataDir)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read data directory %s", w.dataDir)
	}

	var sum float64
	var count float64
	for _, file := range files {
		if file.Name() == IndexFile || file.Name() == ".keep" {
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
	indexPath := w.dataDir + IndexFile
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
		if file.Name() == IndexFile || file.Name() == ".keep" {
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

func (w *Writer) Where() ([]models.Event, error) {
	return []models.Event{}, nil
}
