/*
Copyright 2018 Sgoby.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ustring

import (
	"regexp"
	"strings"
)

func IsNumeric(str string) bool{
	reg,err := regexp.Compile("\\D+?")
	if err !=nil{
		return false
	}
	//
	return reg.MatchString(str)
}

//
func Trim(str string,argus... string ) string{
	str = strings.TrimSpace(str)
	for _,c := range argus{
		str = strings.Trim(str,c)
	}
	return str
}