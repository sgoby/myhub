package range_rule

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
	rangeType string
}
//
func NewRuleRange(rcnf config.Rule) (*RuleRange,error){
	rHash := new(RuleRange)
	rHash.rangeType = RANGE_NUMERIC;
	beginVal := int64(0)
	for _,shcnf := range rcnf.Shards{
		mShard, err := NewShard(shcnf, rHash.rangeType,beginVal,rcnf.Format)
		if err != nil {
			return nil, err
		}
		rHash.shards = append(rHash.shards, mShard)
		_,beginVal = mShard.GetRange()
	}
	return rHash,nil
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
func (this *RuleRange)strconvInt64(expr sqlparser.Expr) (int64,error){
	buf := sqlparser.NewTrackedBuffer(nil)
	expr.Format(buf)
	if this.rangeType == RANGE_NUMERIC {
		startNum, err := strconv.ParseInt(buf.String(), 10, 64)
		if err != nil {
			return 0, err
		}
		return startNum, nil
	}
	if this.rangeType == RANGE_DATE {
		dateStr := buf.String()
		dateStr = strings.Replace(dateStr,"'","",-1)
		dateStr = strings.Replace(dateStr,"\"","",-1)
		//
		valTime,err := time.Parse("2006-01-02 15:04:05", dateStr)
		if err != nil {
			return 0, err
		}
		return valTime.Unix(),nil;
	}
	return 0, fmt.Errorf("Invalid range type of:",this.rangeType);
}
