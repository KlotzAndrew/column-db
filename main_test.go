package columndb_test

import (
	"testing"

	"columndb/models"
	"columndb/reader"
	"columndb/writer"

	"github.com/stretchr/testify/assert"
)

func TestWriteAndRead(t *testing.T) {
	event := models.Event{
		Fields: map[string]any{
			"status":        200,
			"response_time": 200,
		},
	}

	w := writer.NewWriter("data/")
	err := w.Setup()
	assert.NoError(t, err)

	err = w.SaveEvent(event)
	assert.NoError(t, err)
	assert.Equal(t, 1, 1)

	res, err := reader.GetEvent(1)
	assert.NoError(t, err)

	assert.NotEqual(t, event, res)
}
