// store/lru_test.go
package store

import (
	"sync"
	"testing"
	"time"
)

// 测试基本操作
func TestLRUCacheBasic(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 100,
	})

	// 正常 Set 和 Get
	cache.Set("a", ByteView("1"))
	cache.Set("b", ByteView("2"))
	cache.Set("c", ByteView("3"))

	v, ok := cache.Get("a")
	if !ok || v.String() != "1" {
		t.Errorf("expected a=1")
	}

	// 不存在的 key
	v, ok = cache.Get("nonexistent")
	if ok {
		t.Errorf("expected not found")
	}
}

// 测试更新
func TestLRUCacheUpdate(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 100,
	})

	cache.Set("key", ByteView("value1"))
	cache.Set("key", ByteView("value2"))

	v, ok := cache.Get("key")
	if !ok || v.String() != "value2" {
		t.Errorf("expected value2")
	}
}

// 测试删除
func TestLRUCacheDelete(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 100,
	})

	cache.Set("a", ByteView("1"))
	if !cache.Delete("a") {
		t.Errorf("delete should return true")
	}

	if _, ok := cache.Get("a"); ok {
		t.Errorf("a should be deleted")
	}

	// 删除不存在的 key
	if cache.Delete("nonexistent") {
		t.Errorf("delete nonexistent should return false")
	}
}

// 测试容量限制
func TestLRUCacheCapacity(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 10, // 只够存很少的数据
	})

	// 添加超出容量的数据
	cache.Set("key1", ByteView("value1")) // 11 bytes
	cache.Set("key2", ByteView("value2")) // 11 bytes → 超过容量，淘汰 key1
	cache.Set("key3", ByteView("value3")) // 11 bytes → 淘汰 key2

	// key1 应该已被淘汰
	if _, ok := cache.Get("key1"); ok {
		t.Errorf("key1 should be evicted due to capacity limit")
	}

	// key3 应该是最新的
	if _, ok := cache.Get("key3"); !ok {
		t.Errorf("key3 should exist")
	}
}

// 测试淘汰回调
func TestLRUCacheOnEvicted(t *testing.T) {
	var evictedKeys []string
	cache := NewLRUCache(Options{
		MaxBytes: 4,
		OnEvicted: func(key string, value Value) {
			evictedKeys = append(evictedKeys, key)
		},
	})

	cache.Set("a", ByteView("1"))
	cache.Set("b", ByteView("2"))
	cache.Set("c", ByteView("3")) // 触发淘汰
	// time.Sleep(50 * time.Millisecond)

	if len(evictedKeys) != 1 || evictedKeys[0] != "a" {
		t.Errorf("expected evicted key 'a', got %v", evictedKeys)
	}
}

// 测试过期时间
func TestLRUCacheExpiration(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes:        100,
		CleanupInterval: 50 * time.Millisecond,
	})
	defer cache.Close()

	// 设置一个键值对，并设置过期时间为当前时间后 100ms
	key := "testKey"
	value := ByteView("testValue")
	cache.Set(key, value)
	cache.expires[key] = time.Now().Add(100 * time.Millisecond)

	// 等待超过过期时间
	time.Sleep(150 * time.Millisecond)

	// 验证过期后的键值对是否已被移除
	_, ok := cache.Get(key)
	if ok {
		t.Errorf("expected key to be evicted due to expiration, but it still exists")
	}
}

// 测试并发安全
func TestLRUCacheConcurrent(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 1000,
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id%26))
			cache.Set(key, ByteView("value"))
			cache.Get(key)
		}(i)
	}

	wg.Wait()

	// 不应该 panic，说明并发安全
}
