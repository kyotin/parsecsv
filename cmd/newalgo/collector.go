package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

var patterns = []string{
	"Initial",
	"First", "FirstL", "First.L", "First_L", "First-L", "FirstTwofirstletteroflast", "LFirst", "L-First", "L.First", "L_First", "TwofirstletteroflastFirst",
	"Last", "LastF", "Last.F", "Last_F", "Last-F", "TwofirstletteroffirstLast", "FLast", "F-Last", "F.Last", "F_Last", "LastTwofirstletteroffirst",
	"LastFirst", "Last.First", "FirstLast", "First.Last", "First_Last", "First-Last", "Last_First"}

type PatternScore map[string]float64
type PatternTotal map[string]int64
type Domain string
type DomainAggregateInfo struct {
	l            sync.Mutex
	PatternScore PatternScore
	PatternTotal PatternTotal
	Entries      int64
}

func (dmai *DomainAggregateInfo) recalculateAllScore() {
	for p, _ := range dmai.PatternScore {
		dmai.PatternScore[p] = float64(dmai.PatternTotal[p]) / float64(dmai.Entries)
	}
}

func NewPatternScore() PatternScore {
	patternScore := make(PatternScore)
	for _, pattern := range patterns {
		patternScore[pattern] = 0.0
	}

	return patternScore
}

func NewPatternTotal() PatternTotal {
	patternTotal := make(PatternTotal)
	for _, pattern := range patterns {
		patternTotal[pattern] = 0
	}

	return patternTotal
}

func NewDomainAggregateInfo() *DomainAggregateInfo {
	return &DomainAggregateInfo{
		PatternScore: NewPatternScore(),
		PatternTotal: NewPatternTotal(),
		Entries:      0,
	}
}

type Collector struct {
	WriteOutFormat string
	DomainInfo     map[Domain]*DomainAggregateInfo
	l              sync.Mutex
}

func NewCollector(WriteOutFormat string) *Collector {
	return &Collector{
		WriteOutFormat: WriteOutFormat,
		DomainInfo: make(map[Domain]*DomainAggregateInfo),
	}
}

func (c *Collector) Collect(analyzer Analyzer) {
	domain := analyzer.Domain
	pattern := analyzer.Pattern
	c.l.Lock()
	if val, ok := c.DomainInfo[domain]; ok {
		val.l.Lock()
		val.Entries += 1
		val.PatternTotal[pattern] += 1
		val.recalculateAllScore()
		val.l.Unlock()
	} else {
		c.DomainInfo[domain] = NewDomainAggregateInfo()
		val := c.DomainInfo[domain]
		val.l.Lock()
		val.Entries += 1
		val.PatternTotal[pattern] += 1
		val.recalculateAllScore()
		val.l.Unlock()
	}
	c.l.Unlock()
}

func (c *Collector) WriteOut(f *os.File, done chan bool) {
	if c.WriteOutFormat == "json" {
		type JsonOutStruct struct {
			Domain Domain
			PatternScore PatternScore
			Entries int64
		}

		for domain, info := range c.DomainInfo {
			jsonOutStruct := JsonOutStruct{
				Domain:       domain,
				PatternScore: info.PatternScore,
				Entries:      info.Entries,
			}

			if b, err := json.Marshal(jsonOutStruct); err == nil {
				_, _ = fmt.Fprintln(f, string(b))
			}
		}
	}

	if c.WriteOutFormat == "csv" {
		for domain, info := range c.DomainInfo {
			first := true
			for _, p := range patterns {
				if first {
					_, _ = fmt.Fprintf(f, "%.2f,%s", info.PatternScore[p], p)
					first = false
				} else {
					_, _ = fmt.Fprintf(f, ",%.2f,%s", info.PatternScore[p], p)
				}
			}
			_, _ = fmt.Fprintf(f, ",%s,%d\n", domain, info.Entries)
		}
	}
	close(done)
}
