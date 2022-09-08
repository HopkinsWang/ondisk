SET CGO_ENABLED=0
SET GOOS=linux
SET GOARCH=amd64
go build -v -o raftondisk .\main.go .\diskkv.go .\execScript.go .\raftdserver.go .\ondisk.go