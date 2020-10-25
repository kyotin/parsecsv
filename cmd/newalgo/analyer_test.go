package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestAnalyzer_AnalysePattern(t *testing.T) {
	testCases := [][]string{
		{"tin", "nguyen", "tn@gmail.com", "Initial"},
		{"tin", "nguyen", "tin@gmail.com", "First"},
		{"tin", "", "tin@gmail.com", "First"},
		{"", "nguyen", "nguyen@gmail.com", "Last"},
		{"tin", "nguyen", "tinnguyen@gmail.com", "FirstLast"},
		{"tin", "nguyen", "tin-nguyen@gmail.com", "First-Last"},
		{"tin", "nguyen", "tin.nguyen@gmail.com", "First.Last"},
		{"tin", "nguyen", "tin_nguyen@gmail.com", "First_Last"},
		{"tin", "nguyen", "ntin@gmail.com", "LFirst"},
		{"tin", "nguyen", "tinn@gmail.com", "FirstL"},
		{"tin", "nguyen", "tin.n@gmail.com", "First.L"},
		{"tin", "nguyen", "tin-n@gmail.com", "First-L"},
		{"tin", "nguyen", "tin_n@gmail.com", "First_L"},
		{"tin", "nguyen", "tinng@gmail.com", "FirstTwofirstletteroflast"},
		{"tin", "nguyen", "ngtin@gmail.com", "TwofirstletteroflastFirst"},
		{"tin", "nguyen", "nguyen@gmail.com", "Last"},
		{"tin", "nguyen", "nguyentin@gmail.com", "LastFirst"},
		{"tin", "nguyen", "nguyen-tin@gmail.com", "Last-First"},
		{"tin", "nguyen", "nguyen.tin@gmail.com", "Last.First"},
		{"tin", "nguyen", "nguyen_tin@gmail.com", "Last_First"},
		{"tin", "nguyen", "nguyent@gmail.com", "LastF"},
		{"tin", "nguyen", "nguyen-t@gmail.com", "Last-F"},
		{"tin", "nguyen", "nguyen.t@gmail.com", "Last.F"},
		{"tin", "nguyen", "nguyen_t@gmail.com", "Last_F"},
		{"tin", "nguyen", "nguyenti@gmail.com", "LastTwofirstletteroffirst"},
		{"tin", "nguyen", "tnguyen@gmail.com", "FLast"},
		{"tin", "nguyen", "t-nguyen@gmail.com", "F-Last"},
		{"tin", "nguyen", "tinguyen@gmail.com", "TwofirstletteroffirstLast"},
	}

	for _, row := range testCases {
		analyzer := Analyzer{
			email:     row[2],
			firstname: row[0],
			lastname:  row[1],
		}

		analyzer.AnalysePattern()

		assert.Nil(t, analyzer.Err, "error...", analyzer.Err)
		assert.Equal(t, row[3], analyzer.Pattern)
	}


}
