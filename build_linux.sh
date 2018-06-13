#!/bin/bash
dir=`pwd`
export GOPATH=$dir
echo $GOPATH
git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
go build -o bin/myhub src/github.com/sgoby/myhub/cmd/myhub/main.go
echo Congratulations. Build success!
