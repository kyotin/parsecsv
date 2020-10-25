package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"parsecsv/internal/model/jsonstruct"
	"parsecsv/internal/reader"
	"parsecsv/internal/utils"
	"sync"
)

var (
	inJson         = flag.String("inJson", "/Users/tinnguyen/Downloads/test.json", "path to json file")
	out            = flag.String("out", "./out.json", "path to out template file")
	readWorkers    = flag.Int("readWorkers", 10, "number of worker for reading")
	buffLines      = flag.Int("buffLines", 2000, "buffer lines when reading")
	processWorkers = flag.Int("workers", 1, "max number of workers")
)

func main() {
	flag.Parse()

	in, err := os.Open(*inJson)
	if err != nil {
		log.Fatal(err)
	}

	out, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}

	lines := make(chan string, *buffLines)

	// Read worker
	var readWaitGroup sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(in, lines, *readWorkers, &readWaitGroup)
	concurrentReader.Read()


	goodLines := make(chan string, *processWorkers)
	// Write worker
	go func(goodLines <-chan string) {
		hmap := make(map[string]struct{})
		for line := range goodLines {
			hashValue := utils.HashSHA1(line)
			if _, ok := hmap[hashValue]; !ok {
				hmap[hashValue] = struct{}{}
				_, _ = out.WriteString(line + "\n")
			}
		}
	}(goodLines)

	// Process workers
	var processWaitGroup sync.WaitGroup
	for i := 0; i < *processWorkers; i++ {
		processWaitGroup.Add(1)
		go func(workerId int, lines <-chan string, goodLines chan<- string, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {
				numOfLines += 1
				record := &jsonstruct.Record{}
				if err := json.Unmarshal([]byte(line), record); err == nil {
					// DO business here
					if b, err := json.Marshal(record.Source); err == nil {
						goodLines <- string(b)
					} else {
						fmt.Println(err)
					}
				}
			}

			fmt.Printf("Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, lines, goodLines, &processWaitGroup)
	}

	readWaitGroup.Wait()
	close(lines)

	processWaitGroup.Wait()
	close(goodLines)

	_ = in.Close()
	_ = out.Close()
}
