package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	db2 "parsecsv/internal/db"
	"parsecsv/internal/model/csvstruct"
	"parsecsv/internal/model/emailpattern"
	"parsecsv/internal/reader"
	"sync"
)

var (
	inCsv        = flag.String("inCsv", "/Users/tinnguyen/go/src/parsecsv/config/newalgo_verified_email.csv", "path to csv file")
	workers      = flag.Int("workers", 1, "max number of workers")
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

	csvFile, err := os.Open(*inCsv)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = csvFile.Close()
	}()

	lines := make(chan string, *buffLines)
	var readWG sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(csvFile, lines, 10, &readWG)
	concurrentReader.Read()

	ctx := context.Background()
	connectService := db2.NewMysqlConnector(ctx, *workers, *workers/2)
	db, err := connectService.Connect(dbConfig.Username, dbConfig.Password, dbConfig.Uri, dbConfig.Database)
	defer connectService.Disconnect()

	dataService := db2.NewDataService(ctx, db)

	var lock sync.Mutex
	processedDomain := make(map[string]struct{})

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerId int, lines <-chan string, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)

			numOfLines := 0
			for line := range lines {
				numOfLines += 1

				csvEmailPattern, err := csvstruct.NewEmailPatternFromLine(line)
				if err != nil {
					fmt.Printf("Read line error %s \n", err)
					continue
				}

				lock.Lock()
				if _, ok := processedDomain[csvEmailPattern.Domain]; ok {
					lock.Unlock()
					continue
				} else {
					processedDomain[csvEmailPattern.Domain] = struct{}{}
					lock.Unlock()
				}

				scores, patterns := csvEmailPattern.First3HighestScorePatterns()
				dbEmailPattern := emailpattern.EmailPattern{
					Score1:     scores[0] * 100.0,
					Pattern1:   patterns[0],
					Score2:     scores[1] * 100.0,
					Pattern2:   patterns[1],
					Score3:     scores[2] * 100.0,
					Pattern3:   patterns[2],
					DomainName: csvEmailPattern.Domain,
					Entry:      csvEmailPattern.Entries,
				}

				if updateRows, err := dataService.UpdateDomainToOld(dbEmailPattern.DomainName); err == nil {
					if insertRows, err := dataService.InsertNewEmailPattern(dbEmailPattern); err == nil {
						fmt.Printf("update %d and insert %d for domain %s, below is rollback cmd \n", updateRows, insertRows, dbEmailPattern.DomainName)
						fmt.Printf("DELETE FROM email_pattern WHERE domain_name=\"%s\";", dbEmailPattern.DomainName)
						if updateRows > 0 {
							fmt.Printf("UPDATE email_pattern SET domain_name=\"%s\" WHERE domain_name=\"%s\";\n", dbEmailPattern.DomainName, "old_" + dbEmailPattern.DomainName)
						}
					} else {
						fmt.Printf("can't insert new email pattern %s \n", err)
					}
				} else {
					fmt.Printf("can't update domain to old %s \n", err)
				}

			}

			fmt.Printf("Worker %d had procesed %d lines \n", workerId, numOfLines)
			wg.Done()
		}(i, lines, &wg)
	}

	readWG.Wait()
	close(lines)

	wg.Wait()
}
