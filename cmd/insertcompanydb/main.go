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
	"runtime"
	"sync"
)

var (
	companyDB  = flag.String("companyDB", "./companydb.json", "path to companydb json")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
	configFolder = flag.String("configFolder", "./config/", "Path to config file")
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

	workers := runtime.NumCPU()

	originJson, err := os.Open(*companyDB)
	if err != nil {
		log.Fatal(err)
	}
	defer originJson.Close()

	dbConfig := &db2.DatabaseConfig{}
	err = viper.UnmarshalKey("database", dbConfig)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	connectService := db2.NewMysqlConnector(ctx, workers, workers/2)
	db, err := connectService.Connect(dbConfig.Username, dbConfig.Password, dbConfig.Uri, dbConfig.Database)
	defer connectService.Disconnect()

	dataService := db2.NewDataService(ctx, db)

	lines := make(chan string, *buffLines)
	var readWG sync.WaitGroup
	concurrentReader := reader.NewConcurrentReader(originJson, lines, workers/2, &readWG)
	concurrentReader.Read()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerId int, wg *sync.WaitGroup) {
			fmt.Printf("Worker %d Start \n", workerId)
			numOfLines := 0
			for line := range lines {
				numOfLines += 1
				company := &jsonstruct.CompanyDB{}
				if err := json.Unmarshal([]byte(line), company); err == nil {
					_, _ = dataService.InsertNewCompany(company)
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