CGO_ENABLED=0  GOOS=linux GOARCH=amd64
go build -o ./target/redigo-linux main.go