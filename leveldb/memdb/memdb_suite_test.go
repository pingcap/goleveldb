package memdb

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/testutil"
	"github.com/pingcap/goleveldb/leveldb/util"
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
				if db.rnd.src.Int63()%5 == 0 {
					db.rnd.src.Seed(db.rnd.src.Int63())
				}
			}

		}(db, &wg)
	}
	wg.Wait()
}

func TestMergeSize(t *testing.T) {
	db1 := New(comparer.DefaultComparer, 0)
	db2 := New(comparer.DefaultComparer, 0)

	db1.Put([]byte("1"), []byte("1"))
	db1.Put([]byte("2"), []byte("2"))

	db2.Put([]byte("1"), []byte("111"))
	db2.Put([]byte("2"), nil)
	db2.Put([]byte("3"), []byte("3"))

	calSize, calCount, _ := db1.PreMerge(&dbIter{p: db2, slice: &util.Range{}})

	iter := &dbIter{p: db2, slice: &util.Range{}}
	iter.Release()
	iter.Next()
	for iter.Valid() {
		if iter.Value() == nil {
			db1.Delete(iter.key)
		} else {
			db1.Put(iter.key, iter.value)
		}
		iter.Next()
	}
	if calSize != db1.kvSize {
		t.Fatalf("precal: %d but got %d", calSize, db1.kvSize)
	}
	if calCount != db1.Len() {
		t.Fatalf("precal: %d but got %d", calCount, db1.Len())
	}
}

func TestBitRand(t *testing.T) {
	src := rand.NewSource(int64(time.Now().Nanosecond()))
	rnd := &bitRand{
		src: src,
	}
	var slot [4]int

	for i := 0; i < 100000; i++ {
		slot[rnd.bitN(2)]++
	}

	sum := 0
	for i := 0; i < 4; i++ {
		x := slot[i] - 25000
		sum += x * x

		if sum >= 200000 {
			t.Fatalf("not so random %d! %d %d %d %d", sum, slot[0], slot[1], slot[2], slot[3])
		}
	}
}
