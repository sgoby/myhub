package stmt

import (
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/config"
	"strconv"
	"github.com/sgoby/myhub/core/rule/result"
	"github.com/golang/glog"
	"fmt"
	"time"
	"strings"
)

type RuleRange struct {
	shards    []*Shard
	strconvInt64 func(expr sqlparser.Expr) (int64,error)
	rangeType string
}
//
func NewRuleRange(rcnf config.Rule) (*RuleRange,error){
	pRange := new(RuleRange)
	pRange.rangeType = RANGE_NUMERIE;
	beginVal := int64(0)
	pRange.strconvInt64 = pRange.strconvInt64Entity
	for _,shcnf := range rcnf.Shards{
		mShard, err := NewShard(shcnf, pRange.rangeType,beginVal,rcnf.Format)
		if err != nil {
			return nil, err
		}
		pRange.shards = append(pRange.shards, mShard)
		_,beginVal = mShard.GetRange()
	}
	return pRange,nil
}
//
func (this *RuleRange)GetShardRule(expr sqlparser.Expr) (rResults []result.RuleResult, err error){
	if expr == nil{
		return this.GetAllShardRule()
	}
	rResults,_,err = this.getSingleShardRule(expr)
	return
}
//
func (this *RuleRange)getSingleShardRule(expr sqlparser.Expr) (rResults []result.RuleResult,ok bool, err error){
	switch pExpr := expr.(type) {
	case *sqlparser.AndExpr:
		glog.Info(expr,"AndExpr")
	case *sqlparser.OrExpr:
		glog.Info(expr,"OrExpr")
	case *sqlparser.NotExpr:
		glog.Info(expr,"NotExpr")
	case *sqlparser.ParenExpr:
		glog.Info(expr,"ParenExpr")
	case *sqlparser.ComparisonExpr:
		glog.Info(expr,"ComparisonExpr")
		switch pExpr.Operator {
		case sqlparser.EqualStr: // =
			rResults,err = this.getTbSuffixEq(pExpr.Right)
			if len(rResults) > 0 {
				ok = true
			}
		case sqlparser.LessThanStr,sqlparser.LessEqualStr: // <, <=
			rResults,err = this.getTbSuffixLte(pExpr.Right)
			if len(rResults) > 0 {
				ok = true
			}
		case sqlparser.GreaterThanStr,sqlparser.GreaterEqualStr: //>,>=
			rResults,err = this.getTbSuffixGte(pExpr.Right)
			if len(rResults) > 0 {
				ok = true
			}
		case sqlparser.InStr:
			return this.getSingleShardRule(pExpr.Right)
		}
		return
	case *sqlparser.RangeCond:
		glog.Info(expr,"RangeCond")
		if pExpr.Operator == sqlparser.BetweenStr {
			fromResults,fromErr := this.getTbSuffixGte(pExpr.From)
			if fromErr != nil{
				err = fromErr
				return
			}
			toResults,toErr:= this.getTbSuffixLte(pExpr.To)
			if toErr != nil{
				err = toErr
				return
			}
			rResults = this.intersectionResults(fromResults, toResults)
			if len(rResults) > 0 {
				ok = true
			}
		}else if pExpr.Operator == sqlparser.NotBetweenStr{
			fromResults,fromErr := this.getTbSuffixGte(pExpr.To) // >
			if fromErr != nil{
				err = fromErr
				return
			}
			toResults,toErr:= this.getTbSuffixLte(pExpr.From) // <
			if toErr != nil{
				err = toErr
				return
			}
			rResults = this.unionResults(fromResults, toResults)
			if len(rResults) > 0 {
				ok = true
			}
		}
		return
	case *sqlparser.IsExpr:
		glog.Info(expr,"IsExpr")
	case *sqlparser.ExistsExpr:
		glog.Info(expr,"ExistsExpr")
	case *sqlparser.SQLVal:
		glog.Info(pExpr,"SQLVal")
		rResults,err = this.getTbSuffixEq(pExpr)
		if len(rResults) > 0 {
			ok = true
		}
		return
	case *sqlparser.NullVal:
		glog.Info(expr,"NullVal")
		return
	case sqlparser.BoolVal:
		glog.Info(expr,"BoolVal")
	case *sqlparser.ColName:
		glog.Info(expr,"ColName")
	case sqlparser.ValTuple:
		glog.Info(pExpr,"ValTuple")
		for _,ex := range pExpr{
			rArr,ok,err := this.getSingleShardRule(ex)
			if err != nil{
				return nil,false,err
			}
			if !ok{ //not in range
				continue
			}
			rResults = append(rResults,rArr...)
		}
		return
	case *sqlparser.Subquery:
		glog.Info(expr,"Subquery")
	case sqlparser.ListArg:
		glog.Info(expr,"ListArg")
	case *sqlparser.BinaryExpr:
		glog.Info(expr,"BinaryExpr")
	case *sqlparser.UnaryExpr:
		glog.Info(expr,"UnaryExpr")
	case *sqlparser.IntervalExpr:
		glog.Info(expr,"IntervalExpr")
	case *sqlparser.CollateExpr:
		glog.Info(expr,"CollateExpr")
	case *sqlparser.FuncExpr:
		glog.Info(expr,"FuncExpr")
	case *sqlparser.CaseExpr:
		glog.Info(expr,"CaseExpr")
	case *sqlparser.ValuesFuncExpr:
		glog.Info(expr,"ValuesFuncExpr")
	case *sqlparser.ConvertExpr:
		glog.Info(expr,"ConvertExpr")
	case *sqlparser.ConvertUsingExpr:
		glog.Info(expr,"ConvertUsingExpr")
	case *sqlparser.MatchExpr:
		glog.Info(expr,"MatchExpr")
	case *sqlparser.GroupConcatExpr:
		glog.Info(expr,"GroupConcatExpr")
	case *sqlparser.Default:
	default:
		glog.Info(pExpr)
	}
	//
	return
}
//交集 = intersection; 并集 = union; 补集 = complement.
func (this *RuleRange)intersectionResults(v1,v2 []result.RuleResult) []result.RuleResult{
	var nResults []result.RuleResult
	for _,r1 := range v1{
		for _,r2 := range v2{
			nR,ok :=  r1.Intersection(&r2)
			if ok{
				nResults = append(nResults,*nR)
			}
		}
	}
	return nResults
}
func (this *RuleRange)unionResults(v1,v2 []result.RuleResult) []result.RuleResult{
	nResults := append(v1,v2...)
	var pResults []result.RuleResult
	for i,r1 := range nResults{
		for j := i ;j < len(nResults);j++{
			if !r1.Equal(&nResults[j]){
				pResults = append(pResults,r1)
			}
		}
	}
	return pResults
}
//======================================================================
//获取大于 val 的表后缀 ex: >= 50
func (this *RuleRange) getTbSuffixGte(expr sqlparser.Expr) (rsArr []result.RuleResult,err error){
	valNum,err := this.strconvInt64(expr)
	if err != nil{
		return nil,err
	}
	for _,sh := range this.shards{
		if rs := sh.getTbSuffixGte(valNum);!rs.IsEmpty(){
			rsArr = append(rsArr,rs)
		}
	}
	return
}
//获取小于 val 的表后缀 ex: <= 50
func (this *RuleRange) getTbSuffixLte(expr sqlparser.Expr) (rsArr []result.RuleResult,err error){
	valNum,err := this.strconvInt64(expr)
	if err != nil{
		return nil,err
	}
	for _,sh := range this.shards{
		if rs := sh.getTbSuffixLte(valNum);!rs.IsEmpty(){
			rsArr = append(rsArr,rs)
		}
	}
	return
}
//获取小于 val 的表后缀 ex: == 50
func (this *RuleRange) getTbSuffixEq(expr sqlparser.Expr) (rsArr []result.RuleResult,err error){
	valNum,err := this.strconvInt64(expr)
	if err != nil{
		return nil,err
	}
	for _,sh := range this.shards{
		if rs := sh.getTbSuffixIn(valNum);!rs.IsEmpty(){
			rsArr = append(rsArr,rs)
		}
	}
	return
}
//========================================================================
//
func (this *RuleRange) getNodeDbNameTbName(val int64) (rs result.RuleResult,isIn bool){
	for _,sh := range this.shards{
		if sh.inShard(val){
			rs = sh.getTbSuffixIn(val)
			return rs,true
		}
	}
	return result.RuleResult{},false
}
//get all shard table
func (this *RuleRange) GetAllShardRule()(rResults []result.RuleResult, err error){
	for _,sh := range this.shards{
		rs := sh.getTbSuffixAll()
		rResults = append(rResults,rs)
	}
	return
}
//
func (this *RuleRange)strconvInt64Entity(expr sqlparser.Expr) (int64,error){
	buf := sqlparser.NewTrackedBuffer(nil)
	expr.Format(buf)
	if this.rangeType == RANGE_NUMERIE{
		startNum, err := strconv.ParseInt(buf.String(), 10, 64)
		if err != nil {
			return 0, err
		}
		return startNum, nil
	}
	if this.rangeType == RANGE_DATA {
		var err error
		dateTimeStr := buf.String()
		dateTimeStr = strings.TrimSpace(dateTimeStr)
		dateTimeStr = strings.Trim(dateTimeStr,"'")
		dateTimeStr = strings.Trim(dateTimeStr,"\"")
		dateTimeStr,err = optimizationDatetime(dateTimeStr);
		if err != nil {
			return 0, err
		}
		deferLayout := "2006-01-02 15:04:05"
		if len(dateTimeStr) > len(deferLayout){
			return 0, fmt.Errorf("Invalid date of:",dateTimeStr)
		}
		valTime,err := time.Parse(deferLayout[0:len(dateTimeStr)], dateTimeStr)
		if err != nil {
			return 0, err
		}
		return valTime.Unix(),nil;
	}
	return 0, fmt.Errorf("Invalid range type of:",this.rangeType);
}
//
func optimizationDatetime(dateTimeStr string) (string,error){
	dtArr := strings.Split(dateTimeStr," ")
	dateStr := dtArr[0]
	var timeStr string;
	if len(dtArr) > 1{
		timeStr = dtArr[1]
	}
	dateArr := strings.Split(dateStr,"-")
	var timeArr []string
	if len(timeStr) > 0{
		timeArr = strings.Split(timeStr,":")
	}
	//
	if len(dateArr[0]) < 4{
		return "",fmt.Errorf("Invalid date of:",dateTimeStr);
	}
	if len(dateArr) > 1 && len(dateArr[1]) < 2{
		dateArr[1] = "0"+dateArr[1]
	}
	if len(dateArr) > 2 && len(dateArr[2]) < 2{
		dateArr[2] = "0"+dateArr[2]
	}
	dateStr = strings.Join(dateArr,"-")
	//
	if len(timeArr) > 0 {
		for i, _ := range timeArr {
			if len(timeArr[i]) < 2 {
				timeArr[i] = "0" + timeArr[i]
			}
		}
		timeStr = strings.Join(timeArr,":")
		return dateStr + " "+timeStr,nil;
	}
	return dateStr,nil
}
