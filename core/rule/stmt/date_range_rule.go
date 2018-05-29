package stmt


import (
	"github.com/sgoby/myhub/config"
)

//
func NewRuleDateRange(rcnf config.Rule) (*RuleRange,error){
	pRange := new(RuleRange)
	pRange.rangeType = RANGE_DATA;
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
