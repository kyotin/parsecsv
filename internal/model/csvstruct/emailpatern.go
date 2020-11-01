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

func (emailPattern EmailPattern) First3HighestScorePatterns() (score []float64, pattern []string){
	max1 := 0.0
	idx1 := -1

	max2 := 0.0
	idx2 := -1

	max3 := 0.0
	idx3 := -1

	for idx, score := range emailPattern.Score {
		if score > max1 {
			max3 = max2
			idx3 = idx2

			max2 = max1
			idx2 = idx1

			max1 = score
			idx1 = idx

			continue
		}

		if score > max2 {
			max3 = max2
			idx3 = idx2

			max2 = score
			idx2 = idx

			continue
		}

		if score > max3 {
			max3 = score
			idx3 = idx

			continue
		}
	}

	score = make([]float64, 3)
	pattern = make([]string, 3)

	if idx1 == -1 {
		for i:=0; i<3;i++ {
			score[i] = emailPattern.Score[i]
			pattern[i] = emailPattern.Pattern[i]
		}
		return
	} else {
		score[0] = emailPattern.Score[idx1]
		pattern[0] = emailPattern.Pattern[idx1]
	}

	if idx2 == -1 {
		walk := 1
		for i:=0; i<30;i++ {
			if i != idx1 && emailPattern.Score[i] == 0.0{
				score[walk] = emailPattern.Score[i]
				pattern[walk] = emailPattern.Pattern[i]
				walk++
			}
			if walk == 3 {
				break
			}
		}
		return
	} else {
		score[1] = emailPattern.Score[idx2]
		pattern[1] = emailPattern.Pattern[idx2]
	}

	if idx3 == -1 {
		walk := 2
		for i:=0; i<30;i++ {
			if i != idx1 && i != idx2 && emailPattern.Score[i] == 0.0{
				score[walk] = emailPattern.Score[i]
				pattern[walk] = emailPattern.Pattern[i]
				walk++
			}
			if walk == 3 {
				break
			}
		}
		return
	} else {
		score[2] = emailPattern.Score[idx3]
		pattern[2] = emailPattern.Pattern[idx3]
	}

	return
}