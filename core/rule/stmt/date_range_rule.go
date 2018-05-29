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
