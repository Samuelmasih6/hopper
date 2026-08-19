allgood:
	go build ./... 2>&1 && echo "BUILD OK" && go vet ./... 2>&1 && echo "VET OK"
