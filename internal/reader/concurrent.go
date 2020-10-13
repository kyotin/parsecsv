package reader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"
)

type ConcurrentReader struct {
	File        *os.File
	Line        chan<- string
	NumOfWorker int
	Wg          *sync.WaitGroup
}

func NewConcurrentReader(file *os.File, line chan<- string, numOfWorkers int, wg *sync.WaitGroup) *ConcurrentReader {
	return &ConcurrentReader{
		File:        file,
		Line:        line,
		NumOfWorker: numOfWorkers,
		Wg:          wg,
	}
}

func (cr *ConcurrentReader) Read() {
	fInfo, err := cr.File.Stat()
	if err != nil {
		log.Fatal(err)
	}

	size := int64(math.Ceil(float64(fInfo.Size()) / float64(cr.NumOfWorker)))

	//lines := make(chan string, *buffLines)
	for i := 0; i < cr.NumOfWorker; i++ {
		cr.Wg.Add(1)
		go func(workerIdx int, lines chan<- string, chunkSize int64, file *os.File, readWaitGroup *sync.WaitGroup) {
			numOfLines := 0
			// allow overlap 1MB
			offset := int64(workerIdx)*chunkSize - 1024*1024
			if offset < 0 {
				offset = 0
			}
			sectionReader := io.NewSectionReader(file, offset, chunkSize)
			reader := bufio.NewReader(sectionReader)
			for {
				var buffer bytes.Buffer
				endOfFile := false
				for {
					l, isPrefix, err := reader.ReadLine()
					if err == io.EOF {
						endOfFile = true
						break
					}

					buffer.Write(l)
					// If we've reached the end of the line, stop reading.
					if !isPrefix {
						break
					}

					if err != nil && err != io.EOF {
						fmt.Printf("ERROR %v \n", err)
						break
					}
				}

				if endOfFile {
					break
				}

				line := buffer.String()
				if line != "" {
					lines <- line
					numOfLines += 1
				}
			}

			readWaitGroup.Done()
		}(i, cr.Line, size, cr.File, cr.Wg)
	}
}
