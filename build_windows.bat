﻿git clone https://github.com/sgoby/myhub src/github.com/sgoby/myhub
set dir=%cd%
set GOPATH=%GOPATH%;%dir%
go build -o bin/myhub.exe src/github.com/sgoby/myhub/main.go
echo Congratulations. Build success!
@ping 127.0.0.1 -n 3 >nul