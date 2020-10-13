package writer

import (
	"os"
	"parsecsv/internal/utils"
	"sync"
)

type WriteConcurrent struct {
	File        *os.File
	Line        <-chan string
	NumOfWorker int
	Wg          *sync.WaitGroup
	lock        sync.Mutex
}

func NewWriteConcurrent(file *os.File, line <-chan string, workers int, wg *sync.WaitGroup) *WriteConcurrent {
	return &WriteConcurrent{
		File:        file,
		Line:        line,
		NumOfWorker: workers,
		Wg:          wg,
		lock:        sync.Mutex{},
	}
}

func (wc *WriteConcurrent) Write() {
	for i := 0; i < wc.NumOfWorker; i++ {
		wc.Wg.Add(1)
		hmap := make(map[string]struct{})
		go func(goodLines <-chan string, out *os.File, wg *sync.WaitGroup,lock *sync.Mutex) {
			for line := range goodLines {
				hashValue := utils.HashSHA1(line)
				lock.Lock()
				if _, ok := hmap[hashValue]; !ok {
					hmap[hashValue] = struct{}{}
					_, _ = out.WriteString(line + "\n")
				}
				lock.Unlock()
			}
			wg.Done()
		}(wc.Line, wc.File, wc.Wg, &wc.lock)
	}
}
