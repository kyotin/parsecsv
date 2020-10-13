package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"parsecsv/internal/reader"
	"parsecsv/internal/utils"
	"strings"
	"sync"
)

var (
	inJson    = flag.String("inJson", "/Users/tinnguyen/Downloads/test.json", "path to json file")
	inCsv     = flag.String("inCsv", "/Users/tinnguyen/Downloads/test.csv", "Path to csv file")
	out       = flag.String("out", "./out.json", "path to out file")
	workers   = flag.Int("workers", 1, "max number of workers")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
)

// csv
type CsvRecord struct {
	Name  string
	Email string
	Phone string
}

//json
type _Source struct {
	PersonName                   string `json:"person_name"`
	PersonFirstNameUnanalyzed    string `json:"person_first_name_unanalyzed"`
	PersonLastNameUnanalyzed     string `json:"person_last_name_unanalyzed"`
	PersonNameUnanalyzedDowncase string `json:"person_name_unanalyzed_downcase"`
	PersonEmailStatusCd          string `json:"person_email_status_cd"`
	PersonExtrapolatedEmail      string `json:"person_extrapolated_email"`
	PersonEmail                  string `json:"person_email"`
	PersonLinkedinUrl            string `json:"person_linkedin_url"`
	PersonPhone                  string `json:"person_phone"`
	SantizedOrganizationName     string `json:"sanitized_organization_name_unanalyzed"`
	OrganizationName             string `json:"organization_name"`
}

type Record struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Source _Source `json:"_source"`
}

func main() {
	flag.Parse()

	csvFile, err := os.Open(*inCsv)
	if err != nil {
		log.Fatal(err)
	}

	jsonFile, err := os.Open(*inJson)
	if err != nil {
		log.Fatal(err)
	}

	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}

	emailPhoneMap := make(map[string]string)

	// work with csv file and build map
	csvLines := make(chan string, *buffLines)
	var csvReadWG sync.WaitGroup
	csvConcurrentReader := reader.NewConcurrentReader(csvFile, csvLines, 30, &csvReadWG)
	csvConcurrentReader.Read()

	var csvBuildMapWG sync.WaitGroup
	csvBuildMapWG.Add(1)
	go func(lines <-chan string, emailPhoneMap map[string]string, wg *sync.WaitGroup) {
		for line := range lines {
			fields := strings.Split(line, "\t")
			if len(fields) < 3 {
				fmt.Printf("ERROR: %s \n", fields)
				continue
			}

			record := CsvRecord{
				Name:  fields[0],
				Email: fields[1],
				Phone: fields[2],
			}

			if record.Phone != "" && record.Email != "" {
				emailPhoneMap[record.Email] = record.Phone
			}
		}
		wg.Done()
	}(csvLines, emailPhoneMap, &csvBuildMapWG)

	csvReadWG.Wait()
	close(csvLines)

	csvBuildMapWG.Wait()

	// work with json file
	lines := make(chan string, *buffLines)
	var readWaitGroup sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(jsonFile, lines, 15, &readWaitGroup)
	concurrentReader.Read()

	goodLines := make(chan string, *workers)
	go func(goodLines <-chan string, out *os.File) {
		hmap := make(map[uint32]struct{})
		for line := range goodLines {
			if _, ok := hmap[utils.Hash(line)]; !ok {
				hmap[utils.Hash(line)] = struct{}{}
				_, _ = out.WriteString(line + "\n")
			}
		}
	}(goodLines, outFile)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerId int, lines <-chan string, goodLines chan<- string, wg *sync.WaitGroup, emailPhoneMap map[string]string) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			hitEmail := 0
			for line := range lines {
				numOfLines += 1
				record := &Record{}
				if err := json.Unmarshal([]byte(line), record); err == nil {
					// DO business here
					if val, ok := emailPhoneMap[record.Source.PersonEmail]; ok{
						record.Source.PersonPhone = val
						hitEmail += 1
					}
					if b, err := json.Marshal(record.Source); err == nil {
						goodLines <- string(b)
					} else {
						fmt.Println(err)
					}
				}
			}

			fmt.Printf("Worker %d had procesed %d lines, and hit %d email \n", workerId, numOfLines, hitEmail)
			wg.Done()
		}(i, lines, goodLines, &wg, emailPhoneMap)
	}

	readWaitGroup.Wait()
	close(lines)

	wg.Wait()
	close(goodLines)

	_ = csvFile.Close()
	_ = jsonFile.Close()
	_ = outFile.Close()
}

