package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"parsecsv/internal/model/jsonstruct"
	"parsecsv/internal/reader"
	"parsecsv/internal/writer"
	"runtime"
	"strings"
	"sync"
)

var (
	contactdb  = flag.String("contactdb", "/Users/tinnguyen/Downloads/sample_contactdb.json", "path to contactdb json")
	out       = flag.String("out", "./out.json", "path to out file")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
)

func main() {
	flag.Parse()

	workers := runtime.NumCPU()

	contactdb, err := os.Open(*contactdb)
	if err != nil {
		log.Fatal(err)
	}
	defer contactdb.Close()


	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	lines := make(chan string, *buffLines)
	var readWG sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(contactdb, lines, workers/2, &readWG)
	concurrentReader.Read()

	goodLines := make(chan string, 2)
	var writeWaitGroup sync.WaitGroup
	concurrentWriter := writer.NewWriteConcurrent(outFile, goodLines, 2, &writeWaitGroup)
	concurrentWriter.Write()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerId int, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {
				numOfLines += 1
				contact := &jsonstruct.ContactDB{}
				if err := json.Unmarshal([]byte(line), contact); err == nil {
					// DO business here
					if contact.PersonEmail != "" && strings.EqualFold(contact.PersonEmailStatusCd, "Verified") && contact.PersonLinkedinUrl != "" {
						goodLines <- line
					}
				} else {
					fmt.Println("can't unmarshal", err)
				}
			}

			fmt.Printf("Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, &wg)
	}

	readWG.Wait()
	close(lines)
	wg.Wait()
}
