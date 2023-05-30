package utils

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

func GetLastLine(file *os.File) ([]byte, error) {
	stat, _ := file.Stat()
	filesize := stat.Size()

	if filesize == 0 {
		return nil, nil
	}

	i := int64(0)

	end := filesize - i
	foundEnd := false

	start := int64(0) // fill

	buf := make([]byte, 1)
	for {
		i++
		currentIndex := filesize - i
		_, err := file.ReadAt(buf, currentIndex)
		if err != nil {
			// check if EOF error
			if err == io.EOF {
				fmt.Println("==== i", i)
			} else {
				return nil, errors.Wrapf(err, "failed to read from file %s", file.Name())
			}
		}

		fmt.Println("=== reading ", buf[0], i, string(buf[0]), "--close--")

		if buf[0] == '\n' {
			fmt.Println("found end", buf[0] == '\n', i, filesize)
			if !foundEnd {
				foundEnd = true
				end = currentIndex
				fmt.Println("end is ", string(buf[0]), end, filesize, i)
			} else {
				start = currentIndex + 1
				fmt.Println("start is ", string(buf[0]), start, filesize, i)
				break
			}
		} else if filesize == i {
			start = 0
			break
		}
	}
	fmt.Println("start", start, "end", end)
	finalBuf := make([]byte, end-start)
	_, err := file.ReadAt(finalBuf, start)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read from file %s", file.Name())
	}

	return finalBuf, nil
}
