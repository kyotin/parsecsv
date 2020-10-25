package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"parsecsv/internal/model/jsonstruct"
	"parsecsv/internal/reader"
	"sync"
)

var (
	inJson    = flag.String("inJson", "/Users/tinnguyen/Downloads/test.json", "path to json file")
	out       = flag.String("out", "./out.csv", "path to out file")
	workers   = flag.Int("workers", 10, "max number of workers")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
)

func main() {
	flag.Parse()

	jsonFile, err := os.Open(*inJson)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = jsonFile.Close()
	}()

	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = outFile.Close()
	}()

	lines := make(chan string, *buffLines)
	var readWG sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(jsonFile, lines, 10, &readWG)
	concurrentReader.Read()

	collector := NewCollector()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerId int, lines <-chan string, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {
				numOfLines += 1
				record := &jsonstruct.Record{}
				if err := json.Unmarshal([]byte(line), record); err == nil {
					analyzer := Analyzer{
						email:     record.Source.PersonEmail,
						firstname: record.Source.PersonFirstNameUnanalyzed,
						lastname:  record.Source.PersonLastNameUnanalyzed,
					}
					analyzer.AnalysePattern()
					if analyzer.Err == nil {
						collector.Collect(analyzer)
					} else {
						fmt.Printf("ERROR: %s \n", analyzer.Err)
					}
				}
			}

			fmt.Printf("Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, lines, &wg)
	}

	readWG.Wait()
	close(lines)

	wg.Wait()

	done := make(chan bool)
	collector.WriteOut(outFile, done)
	<- done
}
