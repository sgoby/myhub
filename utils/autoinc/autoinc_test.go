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
