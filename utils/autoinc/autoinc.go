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

package autoinc

import (
	"sync"
	"encoding/json"
	"os"
	"io"
	"log"
)
type AutoIncrement struct {
	incKey string
	Start  int64 `json:"start"`
	Step   int64 `json:"step"`
	sync.Mutex
}
//
var incrementMap map[string]*AutoIncrement
var mapLocker *sync.Mutex
var saveChan chan int
var mapFile *os.File
//
func init() {
	incrementMap = make(map[string]*AutoIncrement)
	mapLocker = new(sync.Mutex)
	saveChan = make(chan int, 1)
	initMapFile()
}
//
func initMapFile() error {
	var err error
	mapFile, err = os.OpenFile("inc.json", os.O_RDWR|os.O_CREATE, os.ModeType)
	if err != nil {
		return err
	}
	mJson := json.NewDecoder(mapFile)
	err = mJson.Decode(&incrementMap)
	if err == io.EOF {
		return nil
	}
	return err
}
//
func NewAutoIncrement(key string, start, step int64) (*AutoIncrement) {
	if mInc, ok := incrementMap[key]; ok {
		mInc.Start = start;
		mInc.Step = step;
		return mInc;
	}
	mInc := new(AutoIncrement)
	mInc.Start = start;
	mInc.Step = step;
	incrementMap[key] = mInc
	return mInc;
}
//获取下一个自增长ID
func (this *AutoIncrement) GetNext() int64 {
	this.Lock()
	defer this.Unlock()
	val := this.Start + this.Step
	this.Start = val
	//保存
	save()
	return val
}
//
func GetAutoIncrement(key string) (*AutoIncrement) {
	mapLocker.Lock()
	defer mapLocker.Unlock()
	//
	if mInc, ok := incrementMap[key]; ok {
		return mInc
	}
	return NewAutoIncrement(key, 0, 1)
}
//持久保存
func save() {
	//避免高并发阻塞
	select {
	case saveChan <- 1:
		doSave()
		<-saveChan
		return
	default:
		return
	}
}
//
func doSave(){
	if mapFile == nil  {
		err := initMapFile()
		if err != nil{
			log.Println(err)
		}
		return
	}
	if strBytes, err := json.Marshal(incrementMap); err == nil {
		mapFile.WriteAt(strBytes, 0)
	}
}
func Close() {
	if mapFile != nil {
		mapFile.Close()
	}
}
