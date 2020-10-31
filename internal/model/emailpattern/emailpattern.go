package emailpattern

type EmailPattern struct {
	ID         int64
	Score1     float64
	Pattern1   string
	Score2     float64
	Pattern2   string
	Score3     float64
	Pattern3   string
	DomainName string
	Entry      int64
}
