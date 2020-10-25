package main

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestCollector_Collect(t *testing.T) {
	analyzer := Analyzer{
		Pattern: "LastFirst",
		Domain:  "gmail.com",
	}

	analyzer1 := Analyzer{
		Pattern: "FirstLast",
		Domain:  "gmail.com",
	}

	collector := NewCollector()
	collector.Collect(analyzer)
	collector.Collect(analyzer1)
	collector.Collect(analyzer1)
	collector.Collect(analyzer1)

	for k, v := range collector.DomainInfo {
		assert.Equal(t, Domain("gmail.com"), k)
		assert.Equal(t, int64(4), v.Entries)
		assert.Equal(t, 0.25, v.PatternScore["LastFirst"])
		assert.Equal(t, 0.75, v.PatternScore["FirstLast"])
	}

	f, err := os.Create("tmp.out")
	if err != nil {
		log.Fatal("Can't open tmp out")
	}
	defer f.Close()

	done := make(chan bool)
	collector.WriteOut(f, done)
	<-done
}
