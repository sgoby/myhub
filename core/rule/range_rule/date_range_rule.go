package range_rule


import (
	"github.com/sgoby/myhub/config"
)

//
func NewRuleDateRange(rcnf config.Rule) (*RuleRange,error){
	rHash := new(RuleRange)
	rHash.rangeType = RANGE_DATE;
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
