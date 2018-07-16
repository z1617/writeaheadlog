deps:
	go get -u gitlab.com/NebulousLabs/errors
	go get -u gitlab.com/NebulousLabs/fastrand
	go get -u golang.org/x/crypto/blake2b
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install

test: 
	go test -v -race -tags='testing debug netgo' -timeout=1200s ./...

test-lint:
	gometalinter --disable-all --enable=errcheck --enable=vet --enable=gofmt ./...

cover:
	go test -coverprofile=coverage.out -v -race -tags='testing debug netgo' -timeout=1200s ./...

