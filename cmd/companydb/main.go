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
	"sync"
)

var (
	originJson  = flag.String("originJson", "/Users/tinnguyen/Downloads/sample_v7.json", "path to contactdb json")
	out       = flag.String("out", "./out.json", "path to out file")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
)

func main() {
	flag.Parse()
	workers := runtime.NumCPU()

	originJson, err := os.Open(*originJson)
	if err != nil {
		log.Fatal(err)
	}
	defer originJson.Close()


	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	lines := make(chan string, *buffLines)
	var readWG sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(originJson, lines, workers/2, &readWG)
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
				company := &jsonstruct.RecordWithCompany{}
				if err := json.Unmarshal([]byte(line), company); err == nil {
					// DO business here
					if b, err := json.Marshal(company.Source); err == nil {
						goodLines <- string(b)
					}
				} else {
					fmt.Printf("can't unmarshal: %v, %s \n", err, line)
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