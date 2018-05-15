package ustring

import "regexp"

func IsNumeric(str string) bool{
	reg,err := regexp.Compile("\\D+?")
	if err !=nil{
		return false
	}
	//
	return reg.MatchString(str)
}
