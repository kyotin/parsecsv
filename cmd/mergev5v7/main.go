package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"parsecsv/internal/reader"
	"parsecsv/internal/writer"
	"sync"
)

var (
	inJsonV5  = flag.String("inJsonV5", "/Users/tinnguyen/Downloads/dbv5_1000.json", "path to json v5 file")
	inJsonV7  = flag.String("inJsonV7", "/Users/tinnguyen/Downloads/dbv7_1000.json", "Path to json v7 file")
	out       = flag.String("out", "./out.json", "path to out file")
	workers   = flag.Int("workers", 1, "max number of workers")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
)

//json
type _Source struct {
	PersonName                        string   `json:"person_name"`
	PersonFirstNameUnanalyzed         string   `json:"person_first_name_unanalyzed"`
	PersonLastNameUnanalyzed          string   `json:"person_last_name_unanalyzed"`
	PersonNameUnanalyzedDowncase      string   `json:"person_name_unanalyzed_downcase"`
	PersonEmailStatusCd               string   `json:"person_email_status_cd"`
	PersonExtrapolatedEmailConfidence float32  `json:"person_extrapolated_email_confidence"`
	PersonExtrapolatedEmail           string   `json:"person_extrapolated_email"`
	PersonEmail                       string   `json:"person_email"`
	PersonLinkedinUrl                 string   `json:"person_linkedin_url"`
	PersonPhone                       string   `json:"person_phone"`
	PersonLocalCountry                string   `json:"person_location_country"`
	SanitizedOrganizationName         string   `json:"sanitized_organization_name_unanalyzed"`
	OrganizationName                  string   `json:"organization_name"`
	OrganizationLinkedinNumericalUrls []string `json:"organization_linkedin_numerical_urls"`
	Origin                            string   `json:"origin"`
}

type Record struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Source _Source `json:"_source"`
}

func main() {
	flag.Parse()

	jsonV5, err := os.Open(*inJsonV5)
	if err != nil {
		log.Fatal(err)
	}

	jsonV7, err := os.Open(*inJsonV7)
	if err != nil {
		log.Fatal(err)
	}

	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}

	// The strategy is:
	// - Prioritize V7.
	// - Write if:
	// 1. record had been put into V7 json file
	// 2. If record in V5 and its email hadn't been placed in V7

	// work with jsonV7 file
	emailPhoneMap := make(map[string]string)
	lines := make(chan string, *buffLines)
	var readWaitGroup sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(jsonV7, lines, 15, &readWaitGroup)
	concurrentReader.Read()

	goodLines := make(chan string, *workers*1000)
	var writeWaitGroup sync.WaitGroup
	concurrentWriter := writer.NewWriteConcurrent(outFile, goodLines, 10, &writeWaitGroup)
	concurrentWriter.Write()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerId int, lines <-chan string, goodLines chan<- string, wg *sync.WaitGroup, emailPhoneMap map[string]string) {
			fmt.Printf("V7Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {

				record := &Record{}
				if err := json.Unmarshal([]byte(line), record); err == nil {
					// DO business here
					numOfLines += 1
					emailPhoneMap[record.Source.PersonEmail] = record.Source.PersonPhone
					goodLines <- line
				} else {
					fmt.Println("can't unmarshal", err)
				}
			}

			fmt.Printf("V7Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, lines, goodLines, &wg, emailPhoneMap)
	}

	readWaitGroup.Wait()
	close(lines)
	wg.Wait()

	// work with json V5 file
	linesV5 := make(chan string, *buffLines)
	var readV5WaitGroup sync.WaitGroup
	concurrentV5Reader := reader.NewConcurrentReader(jsonV5, linesV5, 15, &readV5WaitGroup)
	concurrentV5Reader.Read()

	var v5WG sync.WaitGroup
	for i := 0; i < *workers; i++ {
		v5WG.Add(1)
		go func(workerId int, lines <-chan string, goodLines chan<- string, wg *sync.WaitGroup, emailPhoneMap map[string]string) {
			fmt.Printf("V5Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range linesV5 {
				numOfLines += 1
				record := &Record{}
				if err := json.Unmarshal([]byte(line), record); err == nil {
					// DO business here
					if _, ok := emailPhoneMap[record.Source.PersonEmail]; !ok {
						goodLines <- line
					}
				} else {
					fmt.Println("can't unmarshal", err)
				}
			}

			fmt.Printf("V5Worker %d had procesed %d lines \n", workerId, numOfLines)
			v5WG.Done()
		}(i, linesV5, goodLines, &v5WG, emailPhoneMap)
	}

	readV5WaitGroup.Wait()
	close(linesV5)

	v5WG.Wait()

	close(goodLines)
	writeWaitGroup.Wait()

	_ = jsonV7.Close()
	_ = jsonV5.Close()
	_ = outFile.Close()
}
