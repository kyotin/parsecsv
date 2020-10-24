package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"parsecsv/internal/reader"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	inJson    = flag.String("inJson", "/Users/tinnguyen/Downloads/test.json", "path to json file")
	out       = flag.String("out", "./out.json", "path to out template file")
	workers   = flag.String("workers", "1", "max number of workers")
	buffLines = flag.String("buffLines", "100", "buffer lines when reading")
	field     = flag.String("field", "organization_domain", "field want to count distinct")
)

type _Source struct {
	PersonEmailStatusCd    string `json:"person_email_status_cd"`
	OrganizationDomain     string `json:"organization_domain"`
}

type Record struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Source _Source `json:"_source"`
}

func main() {
	flag.Parse()

	in, err := os.Open(*inJson)
	if err != nil {
		panic("Can't open file")
	}
	defer in.Close()

	out, err := os.Create(*out)
	if err != nil {
		panic("Can't create file")
	}
	defer out.Close()

	buffLines, _ := strconv.Atoi(*buffLines)
	lines := make(chan string, buffLines)
	var readWaitGroup sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(in, lines, 15, &readWaitGroup)
	concurrentReader.Read()

	maxWorker, _ := strconv.Atoi(*workers)

	goodLines := make(chan string, maxWorker)
	done := make(chan bool)
	go func(goodLines <-chan string, done <-chan bool) {
		m := make(map[string]int64, 1000)
		for {
			select {
			case line := <-goodLines:
				str := strings.Split(line, ",")
				incr, _ := strconv.Atoi(str[1])
				m[str[0]] = m[str[0]] + int64(incr)
			case <-done:
				for k, v := range m {
					row := fmt.Sprintf("%s, %d \n", k, v)
					_, _ = out.WriteString(row)
				}
				break
			}
		}
	}(goodLines, done)

	var wg sync.WaitGroup
	for i := 0; i < maxWorker; i++ {
		wg.Add(1)
		go func(workerId int, lines <-chan string, goodLines chan<- string, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {
				numOfLines += 1
				record := &Record{}
				err := json.Unmarshal([]byte(line), record)
				if err == nil {
					// DO business
					switch *field {
					case "person_email_status_cd":
						if record.Source.PersonEmailStatusCd == "" {
							goodLines <- "null,1"
						} else {
							goodLines <- record.Source.PersonEmailStatusCd + ",1"
						}
					case "organization_domain":
						if record.Source.OrganizationDomain == "" {
							goodLines <- "null,1"
						} else {
							goodLines <- record.Source.OrganizationDomain + ",1"
						}
					}
					// for another case add more
				}
			}

			fmt.Printf("Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, lines, goodLines, &wg)
	}

	readWaitGroup.Wait()
	close(lines)
	wg.Wait()
	done <- true

	// wait for writing to file
	<-time.After(5 * time.Second)
}
