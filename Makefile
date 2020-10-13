export-go:
	export PATH=$(PATH):/usr/local/go/bin

count-build:
	go build -o csvcount ./cmd/count

merge-build:
	go build -o merge ./cmd/merge