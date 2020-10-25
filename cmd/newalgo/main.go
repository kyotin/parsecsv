package main

import "flag"

var (
	inJson    = flag.String("inJson", "/Users/tinnguyen/Downloads/contactdb.json", "path to contactdb file")
	out       = flag.String("out", "./out.json", "path to out file")
	workers   = flag.Int("workers", 10, "max number of workers")
	buffLines = flag.Int("buffLines", 1000, "buffer lines when reading")
)

func main() {

}
