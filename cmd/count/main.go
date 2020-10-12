package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	in  = flag.String("in", "/Users/tinnguyen/Downloads/test.csv", "Path to input file")
	out = flag.String("out", "./out.csv", "Path to output file")
)

type Record struct {
	Name  string
	Email string
	Phone string
}

type Report struct {
	NumberOfEmails   map[string]struct{}
	NumberOfPhones   map[string]struct{}
	NumberOfPhones33 map[string]struct{}
}

func main() {
	flag.Parse()

	in, err := os.Open(*in)
	if err != nil {
		log.Fatal(err)
	}

	out, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(in)
	r.Comma = getSeparator("\t")

	report := &Report{
		NumberOfEmails:   make(map[string]struct{}),
		NumberOfPhones:   make(map[string]struct{}),
		NumberOfPhones33: make(map[string]struct{}),
	}

	for {
		fields, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		record := Record{
			Name:  fields[0],
			Email: fields[1],
			Phone: fields[2],
		}

		report.NumberOfEmails[record.Email] = struct{}{}
		report.NumberOfPhones[record.Phone] = struct{}{}

		if strings.HasPrefix(record.Phone, "+33") {
			report.NumberOfPhones33[record.Phone] = struct{}{}
		}
	}

	_, _ = out.WriteString(fmt.Sprintf("Number of emails: %d \n", len(report.NumberOfEmails)))
	_, _ = out.WriteString(fmt.Sprintf("Number of phones: %d \n", len(report.NumberOfPhones)))
	_, _ = out.WriteString(fmt.Sprintf("Number of phones33: %d \n", len(report.NumberOfPhones33)))

	defer func() {
		_ = in.Close()
		_ = out.Close()
	}()
}

func getSeparator(sepString string) (sepRune rune) {
	sepString = `'` + sepString + `'`
	sepRunes, err := strconv.Unquote(sepString)
	if err != nil {
		if err.Error() == "invalid syntax" { // Single quote was used as separator. No idea why someone would want this, but it doesn't hurt to support it
			sepString = `"` + sepString + `"`
			sepRunes, err = strconv.Unquote(sepString)
			if err != nil {
				panic(err)
			}

		} else {
			panic(err)
		}
	}
	sepRune = ([]rune(sepRunes))[0]

	return sepRune
}
