package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"parsecsv/internal/reader"
	"parsecsv/internal/writer"
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
	Name                              string   `json:"person_name_unanalyzed_downcase"`
	PersonFirstNameUnanalyzed         string   `json:"person_first_name_unanalyzed"`
	PersonLastNameUnanalyzed          string   `json:"person_last_name_unanalyzed"`
	Email                             string   `json:"person_email"`
	Phone                             string   `json:"person_phone"`
	PersonLinkedinUrl                 string   `json:"person_linkedin_url"`
	SanitizedOrganizationName         string   `json:"sanitized_organization_name_unanalyzed"`
	OrganizationLinkedinNumericalUrls []string `json:"organization_linkedin_numerical_urls"`
	Origin                            string   `json:"origin"`
	PersonLocalCountry                string   `json:"person_location_country"`
}

func (s CsvRecord) IsNotValid() bool {
	return (s.Phone == "" && s.Email == "") ||
		(s.Email == "" && s.PersonLinkedinUrl == "") ||
		(s.Phone == "" && s.PersonLinkedinUrl == "") ||
		(s.Email == "" && s.SanitizedOrganizationName == "")
}

//json
type _Source struct {
	PersonEmail                       string   `json:"person_email"`
	PersonPhone                       string   `json:"person_phone"`
}


type JsonRecord struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Source _Source `json:"_source"`
}

func main() {
	flag.Parse()

	jsonFile, err := os.Open(*inJson)
	if err != nil {
		log.Fatal(err)
	}

	csvFile, err := os.Open(*inCsv)
	if err != nil {
		log.Fatal(err)
	}

	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}

	phoneEmailMap := make(map[string]string)

	// work with json file
	lines := make(chan string, *buffLines)
	var readWaitGroup sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(jsonFile, lines, 10, &readWaitGroup)
	concurrentReader.Read()

	var jsonBuildMapWG sync.WaitGroup
	jsonBuildMapWG.Add(1)
	go func(lines <-chan string, phoneEmailMap map[string]string, wg *sync.WaitGroup) {
		for line := range lines {
			record := &JsonRecord{}
			if err := json.Unmarshal([]byte(line), record); err == nil {
				phoneEmailMap[record.Source.PersonPhone] = record.Source.PersonEmail
			} else {
				fmt.Println("can't unmarshal", err)
			}
		}
		wg.Done()
	}(lines, phoneEmailMap, &jsonBuildMapWG)

	readWaitGroup.Wait()
	close(lines)

	jsonBuildMapWG.Wait()
	fmt.Printf("Build phoneEmailMap finish, len %d \n", len(phoneEmailMap))

	// work with csv file
	csvLines := make(chan string, *buffLines)
	var csvReadWG sync.WaitGroup
	csvConcurrentReader := reader.NewConcurrentReader(csvFile, csvLines, 30, &csvReadWG)
	csvConcurrentReader.Read()

	goodLines := make(chan string, *workers*1000)
	var writeWaitGroup sync.WaitGroup
	concurrentWriter := writer.NewWriteConcurrent(outFile, goodLines, 10, &writeWaitGroup)
	concurrentWriter.Write()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerId int, csvLines <-chan string, goodLines chan<- string, wg *sync.WaitGroup, phoneEmailMap map[string]string) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			hitEmail := 0
			for line := range csvLines {
				numOfLines += 1

				fields := strings.Split(line, "\t")
				if len(fields) < 13 {
					fmt.Printf("ERROR: %s \n", fields)
					continue
				}

				record := CsvRecord{
					Name:  fields[0],
					Email: fields[1],
					Phone: fields[2],
					PersonLinkedinUrl: fields[3],
					SanitizedOrganizationName: fields[5],
					OrganizationLinkedinNumericalUrls: []string{fields[11]},
					PersonLocalCountry: fields[12],
					Origin: "V5",
				}

				if flname:= strings.Split(record.Name, " "); len(flname) == 2 {
					record.PersonFirstNameUnanalyzed = flname[0]
					record.PersonLastNameUnanalyzed = flname[1]
				}

				if record.Email == "null" {
					if val, ok := phoneEmailMap[record.Email]; ok {
						record.Email = val
					}
				}

				if b, err := json.Marshal(record); err == nil {
					goodLines <- string(b)
				} else {
					fmt.Println(err)
				}
			}

			fmt.Printf("Worker %d had procesed %d lines, and hit %d email \n", workerId, numOfLines, hitEmail)
			wg.Done()
		}(i, csvLines, goodLines, &wg, phoneEmailMap)
	}

	csvReadWG.Wait()
	close(csvLines)

	wg.Wait()
	close(goodLines)

	writeWaitGroup.Wait()

	_ = csvFile.Close()
	_ = jsonFile.Close()
	_ = outFile.Close()
}
