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
			"error":         "tea pot",
			"success":       true,
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

func TestContinuesIndex(t *testing.T) {
	clock := clockwork.NewFakeClockAt(time.Unix(500, 100))
	w := writer.NewWriter("data-continues/", clock)
	err := w.Setup()
	assert.NoError(t, err)

	event := models.Event{
		Fields: map[string]any{
			"status":        float64(300),
			"response_time": float64(56.3),
			"error":         "tea pot",
		},
	}

	err = w.SaveEvent(event)
	assert.NoError(t, err)

	clock.Advance(time.Second * 10)

	w = writer.NewWriter("data-continues/", clock)
	err = w.Setup()
	assert.NoError(t, err)

	err = w.SaveEvent(event)
	assert.NoError(t, err)

	expected_1 := models.Event{
		ID:        2,
		Timestamp: 510,
		Fields:    event.Fields,
	}

	res, err := w.GetEvent(expected_1.ID)
	assert.NoError(t, err)
	assert.Equal(t, expected_1, res)
}

func saveEvents(t *testing.T, w *writer.Writer, events []models.Event) {
	for _, event := range events {
		err := w.SaveEvent(event)
		assert.NoError(t, err)
	}
}

func TestAvg(t *testing.T) {
	clock := clockwork.NewFakeClockAt(time.Unix(500, 100))
	w := writer.NewWriter("data/", clock)
	err := w.Setup()
	assert.NoError(t, err)

	saveEvents(t, w,
		[]models.Event{
			{Fields: map[string]any{"duration": float64(100)}},
			{Fields: map[string]any{"duration": float64(200)}},
			{Fields: map[string]any{"duration": float64(300)}},
		},
	)

	avg, err := w.Avg("duration")
	assert.NoError(t, err)

	assert.Equal(t, 0, avg)
}
