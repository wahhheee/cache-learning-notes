package store

import (
	"strconv"
	"testing"
	"time"
)

func newBenchmarkLRU2Store() *lru2Store {
	return newLRU2Cache(Options{
		BucketCount:     64,
		CapPerBucket:    1024,
		Level2Cap:       1024,
		CleanupInterval: time.Hour,
	})
}

func benchmarkValue(i int) ByteView {
	return ByteView("value-" + strconv.Itoa(i))
}

func benchmarkKey(i int) string {
	return "key-" + strconv.Itoa(i)
}

func BenchmarkLRU2Set(b *testing.B) {
	store := newBenchmarkLRU2Store()
	defer store.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = store.Set(benchmarkKey(i), benchmarkValue(i))
	}
}

func BenchmarkLRU2GetHitLevel1(b *testing.B) {
	store := newBenchmarkLRU2Store()
	defer store.Close()

	const key = "hot-key"
	value := ByteView("hot-value")
	_ = store.Set(key, value)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Delete(key)
		_ = store.Set(key, value)
		_, ok := store.Get(key)
		if !ok {
			b.Fatal("expected level1 hit")
		}
	}
}

func BenchmarkLRU2GetHitLevel2(b *testing.B) {
	store := newBenchmarkLRU2Store()
	defer store.Close()

	const key = "warm-key"
	_ = store.Set(key, ByteView("warm-value"))
	if _, ok := store.Get(key); !ok {
		b.Fatal("expected warmup promotion to level2")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ok := store.Get(key)
		if !ok {
			b.Fatal("expected level2 hit")
		}
	}
}

func BenchmarkLRU2GetMiss(b *testing.B) {
	store := newBenchmarkLRU2Store()
	defer store.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ok := store.Get(benchmarkKey(i))
		if ok {
			b.Fatal("expected miss")
		}
	}
}

func BenchmarkLRU2ParallelMixed(b *testing.B) {
	store := newBenchmarkLRU2Store()
	defer store.Close()

	for i := 0; i < 4096; i++ {
		_ = store.Set(benchmarkKey(i), benchmarkValue(i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := benchmarkKey(i & 4095)
			if i&7 == 0 {
				_ = store.Set(key, benchmarkValue(i))
			} else {
				store.Get(key)
			}
			i++
		}
	})
}
