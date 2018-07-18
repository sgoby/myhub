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

package server

import (
	hubclient "github.com/sgoby/myhub/core/client"
)

type ConnectorMap struct {
	table map[int64]*hubclient.Connector
	lock  chan int
}

//
func NewConnectorMap() *ConnectorMap {
	return &ConnectorMap{
		lock: make(chan int, 1),
		table:make(map[int64]*hubclient.Connector),
	}
}
func (cm *ConnectorMap) Len() int {
	return len(cm.table)
}
func (cm *ConnectorMap) Put(c *hubclient.Connector) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	if c == nil{
		return
	}
	cm.table[c.GetConnectionID()] = c
}
func (cm *ConnectorMap) Get(id int64) (c *hubclient.Connector, ok bool) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	c,ok = cm.table[id]
	return
}
func (cm *ConnectorMap) GetSlice() (s []*hubclient.Connector) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	for _,c := range cm.table{
		s = append(s,c)
	}
	return s
}
//
func (cm *ConnectorMap) Del(id int64) (c *hubclient.Connector) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	cc,ok := cm.table[id]
	if ok{
		delete(cm.table,id)
	}
	return cc
}
