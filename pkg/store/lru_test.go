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

func TestLRUCacheTTL(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes:        100,
		CleanupInterval: 10 * time.Millisecond, // 快速清理
	})
	defer cache.Close()

	// 设置一个 50ms 后过期的数据
	cache.SetWithExpiration("short-lived", ByteView("value1"), 50*time.Millisecond)

	// 立刻获取，应该存在
	v, ok := cache.Get("short-lived")
	if !ok || v.String() != "value1" {
		t.Errorf("should exist immediately")
	}

	// 等待 100ms
	time.Sleep(100 * time.Millisecond)

	// 应该已经过期
	v, ok = cache.Get("short-lived")
	if ok {
		t.Errorf("should be expired after 100ms")
	}
}

func TestLRUCacheTTLNeverExpires(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 100,
	})

	// 永不过期
	cache.Set("permanent", ByteView("value"))

	// 等多久都不应该过期
	time.Sleep(100 * time.Millisecond)

	v, ok := cache.Get("permanent")
	if !ok || v.String() != "value" {
		t.Errorf("should still exist")
	}
}

func TestLRUCacheTTLUpdate(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes:        100,
		CleanupInterval: 10 * time.Millisecond,
	})
	defer cache.Close()

	// 设置一个很快过期的
	cache.SetWithExpiration("key", ByteView("old"), 50*time.Millisecond)

	// 用新的值覆盖（新的过期时间）
	cache.SetWithExpiration("key", ByteView("new"), time.Hour)

	// 等 100ms
	time.Sleep(100 * time.Millisecond)

	// 新值应该还在
	v, ok := cache.Get("key")
	if !ok || v.String() != "new" {
		t.Errorf("new value should exist")
	}
}

func TestLRUCacheActiveCleanup(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes:        100,
		CleanupInterval: 50 * time.Millisecond,
	})
	defer cache.Close()

	// 设置多个过期的数据
	cache.SetWithExpiration("expired1", ByteView("v1"), 30*time.Millisecond)
	cache.SetWithExpiration("expired2", ByteView("v2"), 30*time.Millisecond)
	cache.SetWithExpiration("keep", ByteView("v3"), time.Hour)

	// 等待主动清理
	time.Sleep(200 * time.Millisecond)

	// expired1 和 expired2 应该被清理协程删除了
	if _, ok := cache.Get("expired1"); ok {
		t.Errorf("expired1 should be cleaned up")
	}
	if _, ok := cache.Get("expired2"); ok {
		t.Errorf("expired2 should be cleaned up")
	}

	// keep 还在
	if _, ok := cache.Get("keep"); !ok {
		t.Errorf("keep should still exist")
	}
}

func TestLRUCacheCapacityLimit(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 30, // 最多 30 字节
	})
	t.Log("cache len:", cache.Len())

	// 添加 3 个数据，每个约 10 字节
	cache.Set("a", ByteView("aaaaaaaaaa")) // 10 bytes
	t.Log("cache len:", cache.Len())

	cache.Set("b", ByteView("bbbbbbbbbb")) // 10 bytes
	t.Log("cache len:", cache.Len())

	cache.Set("c", ByteView("cccccccccc")) // 10 bytes
	// usedBytes = 30，刚刚好
	t.Log("cache len:", cache.Len())

	// 再添加一个，触发淘汰
	cache.Set("d", ByteView("dddddddddd")) // 10 bytes
	t.Log("cache len:", cache.Len())

	// usedBytes = 40 > 30，淘汰最久的
	// a 应该被淘汰
	if _, ok := cache.Get("a"); ok {
		t.Errorf("a should be evicted")
	}

	// b, c, d 应该还在
	if _, ok := cache.Get("b"); !ok {
		t.Errorf("b should exist")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Errorf("c should exist")
	}
	if _, ok := cache.Get("d"); !ok {
		t.Errorf("d should exist")
	}
}

func TestLRUCacheCapacityUpdate(t *testing.T) {
	cache := NewLRUCache(Options{
		MaxBytes: 20,
	})

	cache.Set("key", ByteView("short"))           // 9 bytes
	cache.Set("key", ByteView("verylongervalue")) // 21 bytes

	// usedBytes 应该变成 21
	// 但 21 > 20，所以应该只保留 key
	// 不会淘汰其他数据（因为只有这一个）
	v, ok := cache.Get("key")
	if !ok {
		t.Errorf("key should exist")
	}
	if v.String() != "verylongervalue" {
		t.Errorf("value should be updated")
	}
}

func TestLRUCacheOnEvictedCalled(t *testing.T) {
	var evicted []string
	cache := NewLRUCache(Options{
		MaxBytes: 20,
		OnEvicted: func(key string, value Value) {
			evicted = append(evicted, key)
		},
	})

	cache.Set("a", ByteView("aaaaaaaaaa")) // 10 bytes
	cache.Set("b", ByteView("bbbbbbbbbb")) // 10 bytes
	cache.Set("c", ByteView("cccccccccc")) // 10 bytes → 淘汰 a

	if len(evicted) != 1 || evicted[0] != "a" {
		t.Errorf("expected evicted ['a'], got %v", evicted)
	}
}

func TestLRUCacheZeroMaxBytes(t *testing.T) {
	// maxBytes = 0 表示不限制容量
	cache := NewLRUCache(Options{
		MaxBytes: 0,
	})

	// 可以无限添加
	for i := 0; i < 26; i++ {
		t.Log("size", cache.Len())
		t.Log("Push Key:", string(rune('a'+i%26)))
		cache.Set(string(rune('a'+i%26)), ByteView("largevalue"))
	}

	if cache.Len() != 26 {
		t.Log(cache.Len())
		t.Errorf("expected 1000 items with no limit")
	}
}
