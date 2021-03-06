export-go:
	export PATH=$(PATH):/usr/local/go/bin

count-build:
	go build -o csvcount ./cmd/count

dbv7-build:
	go build -o dbv7 ./cmd/dbv7

dbv5-build:
	go build -o dbv5 ./cmd/dbv5

mergev5v7-build:
	go build -o mergev5v7 ./cmd/mergev5v7

countdistinctbyfield-build:
	go build -o countdistinctbyfield ./cmd/countdistinctbyfield

newalgo-build:
	go build -o newalgo ./cmd/newalgo

insertdb-build:
	go build -o insertdb ./cmd/insertdb

newdomain-build:
	go build -o newdomain ./cmd/newdomain

linkedinvsverifiedemail-build:
	go build -o linkedinvsverifiedemail ./cmd/linkedinvsverifiedemail

emailvsphoneorlinkedinvsphone-build:
	go build -o emailvsphoneorlinkedinvsphone ./cmd/emailvsphoneorlinkedinvsphone

companydb-build:
	go build -o companydb ./cmd/companydb

insertcompanydb-build:
	go build -o insertcompanydb ./cmd/insertcompanydb