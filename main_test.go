package columndb_test

import (
	"testing"
	"time"

	"columndb/models"
	"columndb/writer"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestWriteAndRead(t *testing.T) {
	clock := clockwork.NewFakeClockAt(time.Unix(200, 100))
	w := writer.NewWriter("data/", clock)
	err := w.Setup()
	assert.NoError(t, err)

	event := models.Event{
		Fields: map[string]any{
			"status":        float64(200),
			"response_time": float64(46.3),
		},
	}

	err = w.SaveEvent(event)
	assert.NoError(t, err)

	clock.Advance(time.Second * 10)

	err = w.SaveEvent(event)
	assert.NoError(t, err)

	expected_1 := models.Event{
		ID:        1,
		Timestamp: 200,
		Fields:    event.Fields,
	}

	res, err := w.GetEvent(expected_1.ID)
	assert.NoError(t, err)
	assert.Equal(t, expected_1, res)

	expected_2 := models.Event{
		ID:        2,
		Timestamp: 210,
		Fields:    event.Fields,
	}

	res, err = w.GetEvent(expected_2.ID)
	assert.NoError(t, err)
	assert.Equal(t, expected_2, res)
}
