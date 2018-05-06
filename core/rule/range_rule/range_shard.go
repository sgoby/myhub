package range_rule

import (
	"strconv"
	"fmt"
	"strings"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/core/rule/result"
)

//
//config ex:<shard nodeDataBase="db1" rowLimit="100000000" between="0-10"/>
type Shard struct {
	config       config.Shard
	nodeDataBase string
	rangeStart   int64                  //开始标记  int ,date
	rangeEnd     int64                  //结束标记
	rangeType    string                 //
	ranges       map[string]*shardRange //201602
}

//for Shard rangeType
const (
	RANGE_DATE    = "date"
	RANGE_NUMERIC = "numeric"
	RANGE_STRING  = "string"
)
//
type shardRange struct {
	start int64
	end   int64
}

//获取大于 val 的表后缀 ex: >= 50
func (this *Shard) getTbSuffixGte(val int64) (result.RuleResult){
	rs := result.RuleResult{
		NodeDB:this.nodeDataBase,
	}
	for suff,r :=  range this.ranges{
		if r.end >= val {
			rs.AddTbSuffix(suff)
		}
	}
	return rs
}
//获取小于 val 的表后缀 ex: <= 50
func (this *Shard) getTbSuffixLte(val int64) (result.RuleResult){
	rs := result.RuleResult{
		NodeDB:this.nodeDataBase,
	}
	for suff,r :=  range this.ranges{
		if  r.start <= val {
			rs.AddTbSuffix(suff)
		}
	}
	return rs
}
//
func (this *Shard) getTbSuffixAll()(result.RuleResult){
	rs := result.RuleResult{
		NodeDB:this.nodeDataBase,
	}
	for k,_ := range this.ranges{
		rs.AddTbSuffix(k)
	}
	return rs
}
// val in range
func (this *Shard) getTbSuffixIn(val int64)(result.RuleResult){
	rs := result.RuleResult{
		NodeDB:this.nodeDataBase,
	}
	for k,v := range this.ranges{
		if v.inRange(val) {
			rs.AddTbSuffix(k)
			break;
		}
	}
	return rs
}
//==================================================================
//
func (this *shardRange) inRange(val int64) bool {
	if val < this.end && val >= this.start {
		return true
	}
	return false
}

//创建一个分片
func NewShard(cnf config.Shard, rangeType string,beginVal int64,format string) (*Shard, error) {
	limitStr := cnf.RowLimit
	sh := &Shard{
		config:       cnf,
		nodeDataBase: cnf.Node,
		rangeType:    rangeType,
		//rangeStr : cnf.RangeExpr,  //25-56
	}
	if rangeType == RANGE_NUMERIC {
		limit, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			return nil, err
		}
		err = sh.parseRangeInt(limit, format,beginVal)
		if err != nil{
			return nil,err
		}

	}
	return sh, nil
}
//
func (this *Shard) GetRange()(int64,int64){
	return this.rangeStart,this.rangeEnd
}
//
func (this *Shard) inShard(val int64) bool{
	if val < this.rangeEnd  && val >= this.rangeStart {
		return true
	}
	return false
}

//
func (this *Shard) parseRangeInt(limit int64, format string,beginVal int64) (error) {
	str := this.config.RangeExpr
	rangeMap := make(map[string]*shardRange)
	var err error
	//
	strArr := strings.Split(str, "-")
	var startNum, endNum int64
	startNum, err = strconv.ParseInt(strArr[0], 10, 64)
	if err != nil {
		return err
	}
	if len(strArr) > 1 {
		endNum, err = strconv.ParseInt(strArr[1], 10, 64)
		if err != nil {
			return err
		}
	}
	if endNum < startNum {
		t := endNum
		endNum = startNum
		startNum = t
	}
	//
	this.rangeStart = beginVal
	this.rangeEnd = ((endNum - startNum)  * limit) + beginVal
	//
	var s, e, index int64
	index = startNum
	for {
		if index >= endNum {
			break
		}
		s = (index - startNum) * limit
		e = (index - startNum + 1) * limit
		sr := &shardRange{
			start: s + beginVal,
			end:   e + beginVal,
		}
		if len(format) <= 0 {
			format = "%d"
		}
		rangeMap[fmt.Sprintf(format, index)] = sr
		index += 1
	}

	this.ranges = rangeMap
	return nil
}
