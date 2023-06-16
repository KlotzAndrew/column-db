package columndb_test

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"columndb/models"
	"columndb/writer"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func makeTmpDir(t *testing.T) string {
	dir, err := ioutil.TempDir("data", "test-data-")
	if err != nil {
		assert.NoError(t, err)
	}
	return dir
}

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

// 15k rows: 501, 2436145 ns/o
func BenchmarkReadWrite(b *testing.B) {
	clock := clockwork.NewFakeClockAt(time.Unix(200, 100))
	w := writer.NewWriter("data/", clock)
	err := w.Setup()
	assert.NoError(b, err)

	event := models.Event{
		Fields: map[string]any{
			"status":        float64(200),
			"response_time": float64(46.3),
			"error":         "tea pot",
			"success":       true,
		},
	}

	for n := 0; n < b.N; n++ {
		err = w.SaveEvent(event)
		assert.NoError(b, err)

		clock.Advance(time.Second * 10)

		_, err := w.GetEvent(n + 1)
		assert.NoError(b, err)
	}
}

func BenchmarkWrite(b *testing.B) {
	clock := clockwork.NewFakeClockAt(time.Unix(200, 100))
	w := writer.NewWriter("data/", clock)
	err := w.Setup()
	assert.NoError(b, err)

	event := models.Event{
		Fields: map[string]any{
			"status":        float64(200),
			"response_time": float64(46.3),
			"error":         "tea pot",
			"success":       true,
		},
	}

	for n := 0; n < b.N; n++ {
		err = w.SaveEvent(event)
		assert.NoError(b, err)
	}
}

func BenchmarkRead(b *testing.B) {
	clock := clockwork.NewFakeClockAt(time.Unix(200, 100))
	w := writer.NewWriter("data/", clock)
	err := w.Setup()
	assert.NoError(b, err)

	event := models.Event{
		Fields: map[string]any{
			"status":        float64(200),
			"response_time": float64(46.3),
			"error":         "tea pot",
			"success":       true,
		},
	}

	err = w.SaveEvent(event)
	assert.NoError(b, err)

	for n := 0; n < b.N; n++ {
		_, err = w.GetEvent(1)
		assert.NoError(b, err)
	}
}

func BenchmarkReadWriteWithStartup(b *testing.B) {
	event := models.Event{
		Fields: map[string]any{
			"status":        float64(200),
			"response_time": float64(46.3),
			"error":         "tea pot",
			"success":       true,
		},
	}

	for n := 0; n < b.N; n++ {
		clock := clockwork.NewFakeClockAt(time.Unix(200, 100))
		w := writer.NewWriter("data/", clock)
		err := w.Setup()
		assert.NoError(b, err)

		err = w.SaveEvent(event)
		assert.NoError(b, err)

		clock.Advance(time.Second * 10)

		_, err = w.GetEvent(n + 1)
		assert.NoError(b, err)
	}
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

	assert.Equal(t, float64(200), avg)
}

func TestWhere(t *testing.T) {
	dir := makeTmpDir(t)
	defer os.RemoveAll(dir)

	clock := clockwork.NewFakeClockAt(time.Unix(500, 100))
	w := writer.NewWriter(dir+"/", clock)
	err := w.Setup()
	assert.NoError(t, err)

	err = w.SaveEvent(models.Event{Fields: map[string]any{"duration": float64(100)}})
	assert.NoError(t, err)
	clock.Advance(time.Second * 10)

	err = w.SaveEvent(models.Event{Fields: map[string]any{"duration": float64(200)}})
	assert.NoError(t, err)
	clock.Advance(time.Second * 10)

	err = w.SaveEvent(models.Event{Fields: map[string]any{"duration": float64(300)}})
	assert.NoError(t, err)
	clock.Advance(time.Second * 10)

	err = w.SaveEvent(models.Event{Fields: map[string]any{"duration": float64(400)}})
	assert.NoError(t, err)
	clock.Advance(time.Second * 10)

	query1 := writer.Query{
		Filters: map[string][]string{},
	}

	events, err := w.Where(query1)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(events))

	query2 := writer.Query{
		Filters: map[string][]string{
			"timestamp": {writer.GreaterThan, strconv.Itoa(int(time.Unix(510, 100).UTC().Unix()))},
		},
	}

	events, err = w.Where(query2)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(events))
}
