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
	"testing"
	"fmt"
	"time"
	"sync"
	"runtime"
)

func Test_Inc(t *testing.T) {
	runtime.GOMAXPROCS(4)
	beginT := time.Now().UnixNano()
	var wg sync.WaitGroup
	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				GetAutoIncrement("test").GetNext()
			} else {
				GetAutoIncrement("test02").GetNext()
			}
		}()
	}
	wg.Wait()
	Close()
	endT := time.Now().UnixNano()
	fmt.Printf("Use time: %d \n", endT-beginT)

}
