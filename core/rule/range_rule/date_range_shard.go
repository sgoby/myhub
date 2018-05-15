package range_rule

import (
	"strings"
	"time"
	"fmt"
	"regexp"
	"strconv"
)

//format Ymd  limit 1D,1M,1Y
func (this *Shard) parseRangeDate(limit,format string) (error) {
	var dateFormat string
	var defaultLimit string;
	for _,c := range strings.Split(format,""){
		switch c {
		case "Y","y":
			dateFormat += "2006"
			defaultLimit = "1Y";
		case "m","M":
			dateFormat += "01"
			defaultLimit = "1M";
		case "d","D":
			dateFormat += "02"
			defaultLimit = "1D";
		default:
			//dateFormat += c
		}
	}
	if len(dateFormat) < 1{
		return fmt.Errorf("Invalid date format");
	}
	reg,err := regexp.Compile("[0-9]+")
	if err != nil{
		return err
	}
	//
	if len(limit) < 1{
		limit = defaultLimit
	}
	numStr := reg.FindString(limit);
	limitNum,err := strconv.ParseInt(numStr,10,64)
	if err != nil{
		return err
	}
	//
	reg,err = regexp.Compile("[YyMmDd]{1}")
	if err != nil{
		return err
	}
	typeStr := reg.FindString(limit);
	var limitYs,limitMs,limitDs int
	switch typeStr {
	case "Y","y":
		limitYs = int(limitNum);
	case "m","M":
		limitMs = int(limitNum);
	case "d","D":
		limitDs = int(limitNum);
	default:
		//dateFormat += c
	}
	str := this.config.RangeExpr
	rangeMap := make(map[string]*shardRange)
	strArr := strings.Split(str, "-")
	if len(strArr) < 2{
		return fmt.Errorf("Invalid date range");
	}
	beginDate,endDate := strArr[0],strArr[1]
	beginTime,err := time.Parse(dateFormat, beginDate)
	if err != nil{
		return err
	}
	endtime,err := time.Parse(dateFormat, endDate)
	if err != nil{
		return err
	}
	if limit == "Y"{
		beginTime.Year()
	}
	//
	var rangeBeginTime time.Time
	var rangeEndTime time.Time
	rangeBeginTime = beginTime
	//
	for{
		if rangeEndTime.Unix() >= endtime.Unix(){
			break;
		}
		rangeEndTime = rangeBeginTime.AddDate(limitYs,limitMs,limitDs)
		//
		sr := &shardRange{
			start: rangeBeginTime.Unix(),
			end:  rangeEndTime.Unix() - 1, //23:59:59
		}
		//
		rangeMap[rangeBeginTime.Format(dateFormat)] = sr
		//
		rangeBeginTime = rangeEndTime;
	}
	this.ranges = rangeMap
	return nil
}

