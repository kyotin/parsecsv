package csvstruct

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewEmailPatternFromLine(t *testing.T) {
	line := "0.00,Initial,1.00,First,0.00,FirstL,0.00,First.L,0.00,First_L,0.00,First-L,0.00,FirstTwofirstletteroflast,0.00,LFirst,0.00,L-First,0.00,L.First,0.00,L_First,0.00,TwofirstletteroflastFirst,0.00,Last,0.00,LastF,0.00,Last.F,0.00,Last_F,0.00,Last-F,0.00,TwofirstletteroffirstLast,0.00,FLast,0.00,F-Last,0.00,F.Last,0.00,F_Last,0.00,LastTwofirstletteroffirst,0.00,LastFirst,0.00,Last.First,0.00,FirstLast,0.00,First.Last,0.00,First_Last,0.00,First-Last,0.00,Last_First,firstfreelance.com,1"

	pattern, err := NewEmailPatternFromLine(line)
	assert.Nil(t, err)
	fmt.Println(pattern)
}

func TestNewEmailPatternFromLine_Wrong(t *testing.T) {
	line := "0.00,Initial,1.00,First,0.00,FirstL,0.00,First.L,0.00,First_L,0.00,First-L,0.00,FirstTwofirstletteroflast,0.00,LFirst,0.00,L-First,0.00,L.First,0.00,L_First,0.00,0.00,Last,0.00,LastF,0.00,Last.F,0.00,Last_F,0.00,Last-F,0.00,TwofirstletteroffirstLast,0.00,FLast,0.00,F-Last,0.00,F.Last,0.00,F_Last,0.00,LastTwofirstletteroffirst,0.00,LastFirst,0.00,Last.First,0.00,FirstLast,0.00,First.Last,0.00,First_Last,0.00,First-Last,0.00,Last_First,firstfreelance.com,1"
	_, err := NewEmailPatternFromLine(line)
	assert.Equal(t, INVALIDINPUTERR, err)
}

func TestEmailPattern_First3HighestScorePatterns(t *testing.T) {
	line := "0.00,Initial,0.14,First,0.14,FirstL,0.00,First.L,0.00,First_L,0.00,First-L,0.00,FirstTwofirstletteroflast,0.00,LFirst,0.00,L-First,0.00,L.First,0.00,L_First,0.00,TwofirstletteroflastFirst,0.13,Last,0.01,LastF,0.00,Last.F,0.00,Last_F,0.00,Last-F,0.00,TwofirstletteroffirstLast,0.36,FLast,0.00,F-Last,0.01,F.Last,0.00,F_Last,0.00,LastTwofirstletteroffirst,0.00,LastFirst,0.00,Last.First,0.19,FirstLast,0.01,First.Last,0.00,First_Last,0.00,First-Last,0.00,Last_First,google.com,13383"

	pattern, err := NewEmailPatternFromLine(line)
	assert.Nil(t, err)

	scores, patterns := pattern.First3HighestScorePatterns()
	assert.Equal(t, 0.36, scores[0])
	assert.Equal(t, 0.19, scores[1])
	assert.Equal(t, 0.14, scores[2])
	assert.Equal(t, "FLast", patterns[0])
	assert.Equal(t, "FirstLast", patterns[1])
	assert.Equal(t, "First", patterns[2])
}