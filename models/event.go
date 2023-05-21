package models

type Event struct {
	ID        int
	Timestamp int
	Fields    map[string]any
}
