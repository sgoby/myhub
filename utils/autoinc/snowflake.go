package autoinc

import (
	"github.com/sony/sonyflake"
	"time"
)

var snowflakeNode *sonyflake.Sonyflake

func init(){
	settings := sonyflake.Settings{StartTime:time.Date(2018, 9, 1, 0, 0, 0, 0, time.UTC)}
	snowflakeNode = sonyflake.NewSonyflake(settings)
}
//
func GetSnowflakeID() (uint64) {
	if snowflakeNode == nil{
		return 0
	}
	v,err := snowflakeNode.NextID()
	if err !=nil{
		return 0
	}
	return v
}
