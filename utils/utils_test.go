package utils_test

import (
	"columndb/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLastLine(t *testing.T) {
	testFile := "fixtures/test.txt"

	file, err := os.Open(testFile)
	assert.NoError(t, err)

	res, err := utils.GetLastLine(file)
	assert.NoError(t, err)

	assert.Equal(t, "2,210", string(res))
}

func TestGetLastLineSingle(t *testing.T) {
	testFile := "fixtures/single.txt"

	file, err := os.Open(testFile)
	assert.NoError(t, err)

	res, err := utils.GetLastLine(file)
	assert.NoError(t, err)

	assert.Equal(t, "7,123", string(res))
}
