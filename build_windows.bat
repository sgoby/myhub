git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
set dir=%cd%
set GOPATH=%GOPATH%;%dir%
go build -o bin/myhub.exe src/github.com/sgoby/myhub/cmd/myhub/main.go
echo Congratulations. Build success!
pause