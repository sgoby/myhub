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
	table []*hubclient.Connector
	lock  chan int
}

//
func NewConnectorMap() *ConnectorMap {
	return &ConnectorMap{
		lock: make(chan int, 1),
	}
}
func (cm *ConnectorMap) Len() int {
	return len(cm.table)
}
func (cm *ConnectorMap) Put(c *hubclient.Connector) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	cm.table = append(cm.table, c)
}
func (cm *ConnectorMap) Get(id int64) (c *hubclient.Connector, ok bool) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	c, _ = cm.find(id)
	if c != nil {
		return c, true
	}
	return nil, false

}
func (cm *ConnectorMap) GetSlice() (s []*hubclient.Connector) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	s = make([]*hubclient.Connector,len(cm.table))
	for i,c := range cm.table{
		s[i] = c
	}
	return s
}
//
func (cm *ConnectorMap) Del(id int64) (c *hubclient.Connector) {
	cm.lock <- 1
	defer func() { <- cm.lock }()
	c, index := cm.find(id)
	if c == nil {
		return nil
	}
	len := len(cm.table)
	copy(cm.table[index:], cm.table[index+1:])
	cm.table = cm.table[:len-1]
	return c
}
func (cm *ConnectorMap) find(id int64) (c *hubclient.Connector, index int) {
	for i, c := range cm.table {
		if c.GetConnectionID() == id {
			return c, i
		}
	}
	return nil, 0
}
