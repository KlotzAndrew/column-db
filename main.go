package columndb

func main() {
	// write path
	// - get next index
	// - write index/timestamp
	// - conditionally crete file for each field
	// - append index/value to field file

	// schema
	// index | timestamp
	// index | value [filename: <field>.<type>]

	// read path, where
	// AVG(<field_1>) WHERE field_2 > X
	// TODO: timestamp filter to index
	// open file field_2
	// scan events after index cutoff
	// check if matches condition
	// if yes, store value + index
	// open field_1
	// fetch all values with index

	// read path, find by index
	// open all files
	// look for index

	// TODO
	// - write data
	// - query using find
	// - query using where
	// - add age-expory
}
