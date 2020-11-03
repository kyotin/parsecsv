package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	db2 "parsecsv/internal/db"
	"parsecsv/internal/model/jsonstruct"
	"parsecsv/internal/reader"
	"strings"
	"sync"
)

var (
	inJson       = flag.String("inJson", "/Users/tinnguyen/Downloads/test.json", "path to json file")
	out          = flag.String("out", "./out.csv", "path to out file")
	workers      = flag.Int("workers", 2, "max number of workers")
	buffLines    = flag.Int("buffLines", 1000, "buffer lines when reading")
	configFolder = flag.String("configFolder", "/Users/tinnguyen/go/src/parsecsv/config/", "Path to config file")
)

func main() {
	flag.Parse()

	viper.SetConfigName("production")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(*configFolder)

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	dbConfig := &db2.DatabaseConfig{}
	err = viper.UnmarshalKey("database", dbConfig)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	connectService := db2.NewMysqlConnector(ctx, *workers, *workers/2)
	db, err := connectService.Connect(dbConfig.Username, dbConfig.Password, dbConfig.Uri, dbConfig.Database)
	defer connectService.Disconnect()

	dataService := db2.NewDataService(ctx, db)

	// get max number of id
	maxID, err := dataService.GetMaxID()
	if err != nil {
		panic(err)
	}

	var dbDomain sync.Map

	idRange := maxID / int64(*workers)
	fmt.Printf("idRange is %d \n", idRange)
	var queryWG sync.WaitGroup
	for i := 0; i < *workers; i++ {
		queryWG.Add(1)
		go func(workerId int, wg *sync.WaitGroup) {
			startRange := idRange * int64(workerId)
			endRange := startRange + idRange
			emails, err := dataService.FindEmailPatternByIDRange(startRange, endRange)
			if err != nil {
				fmt.Printf("Can't get emails id from range [%d, %d]", startRange, endRange)
				return
			}

			for _, email := range emails {
				dbDomain.Store(email.DomainName, nil)
			}

			defer wg.Done()
		}(i, &queryWG)
	}
	queryWG.Wait()

	// read json file
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

	lines := make(chan string, *buffLines)
	var readWG sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(jsonFile, lines, 10, &readWG)
	concurrentReader.Read()

	var wg sync.WaitGroup
	var newDomain sync.Map
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerId int, lines <-chan string, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {
				numOfLines += 1
				record := &jsonstruct.Record{}
				if err := json.Unmarshal([]byte(line), record); err == nil {
					parts := strings.Split(record.Source.PersonEmail, "@")
					if len(parts) != 2 {
						fmt.Sprintf("look like the email is wrong %s \n", record.Source.PersonEmail)
						continue
					}

					domain := parts[1]
					_, ok := dbDomain.Load(domain)
					if ok {
						continue
					}

					if entries, ok := newDomain.Load(domain); ok {
						newDomain.Store(domain, entries.(int64)+1)
					} else {
						newDomain.Store(domain, int64(1))
					}

				} else {
					fmt.Println("can't unmarshal", err)
				}
			}

			fmt.Printf("Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, lines, &wg)
	}

	readWG.Wait()
	close(lines)

	wg.Wait()

	newDomain.Range(func(k, v interface{}) bool {
		fmt.Fprintf(outFile, "%s, %d\n", k, v.(int64))
		return true
	})
}
