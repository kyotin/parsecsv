package csvstruct

import (
	"errors"
	"strconv"
	"strings"
)

var INVALIDINPUTERR = errors.New("invalid input")

type EmailPattern struct {
	Domain  string
	Entries int64
	Score   []float64
	Pattern []string
}

func NewEmailPatternFromLine(line string) (*EmailPattern, error) {
	fields := strings.Split(line, ",")
	if len(fields) != 62 {
		return nil, INVALIDINPUTERR
	}

	entries, err := strconv.Atoi(fields[61])
	if err != nil {
		return nil, INVALIDINPUTERR
	}

	emailPattern := &EmailPattern{
		Domain:  fields[60],
		Entries: int64(entries),
		Score:   make([]float64, 30),
		Pattern: make([]string, 30),
	}

	scoreIdx := 0
	patternIdx := 0
	for i := 0; i < 60; i++ {
		if i%2 == 0 {
			if score, err := strconv.ParseFloat(fields[i], 64); err == nil {
				emailPattern.Score[scoreIdx] = score
				scoreIdx++
			} else {
				return nil, INVALIDINPUTERR
			}
		} else {
			emailPattern.Pattern[patternIdx] = fields[i]
			patternIdx++
		}
	}

	return emailPattern, nil
}
