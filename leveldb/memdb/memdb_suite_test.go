package memdb

import (
	"sync"
	"testing"

	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}

func TestRace(t *testing.T) {
	var wg sync.WaitGroup
	db := New(comparer.DefaultComparer, 0)

	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func(db *DB, wg *sync.WaitGroup) {
			defer wg.Done()

			for i := 0; i < 2000; i++ {
				if rnd.Intn(5) == 0 {
					rnd.Seed(rnd.Int63())
				}
			}

		}(db, &wg)
	}
	wg.Wait()
}
