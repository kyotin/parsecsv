export-go:
	export PATH=$(PATH):/usr/local/go/bin

count-build:
	go build -o csvcount ./cmd/count

dbv7-build:
	go build -o dbv7 ./cmd/dbv7