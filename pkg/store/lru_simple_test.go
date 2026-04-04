// store/lru_simple_test.go
package store

import (
	"testing"
)

func TestLRUCacheBasic(t *testing.T) {
	cache := NewSimpleLRUCache(3)

	// 添加三个元素
	cache.Set("a", ByteView("1"))
	cache.Set("b", ByteView("2"))
	cache.Set("c", ByteView("3"))

	if cache.Len() != 3 {
		t.Errorf("expected len=3, got %d", cache.Len())
	}

	// 访问 "a"，它变成最近使用的
	if v, ok := cache.Get("a"); !ok || v.String() != "1" {
		t.Errorf("expected a=1")
	}

	// 添加 "d"，应该淘汰 "b"（最久未使用）
	cache.Set("d", ByteView("4"))

	// 现在缓存里应该有 a, c, d
	if cache.Len() != 3 {
		t.Errorf("expected len=3, got %d", cache.Len())
	}

	// b 应该被淘汰了
	if _, ok := cache.Get("b"); ok {
		t.Errorf("b should be evicted")
	}

	// a, c, d 应该还在
	if _, ok := cache.Get("a"); !ok {
		t.Errorf("a should exist")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Errorf("c should exist")
	}
	if _, ok := cache.Get("d"); !ok {
		t.Errorf("d should exist")
	}
}

func TestLRUCacheUpdate(t *testing.T) {
	cache := NewSimpleLRUCache(2)

	cache.Set("a", ByteView("1"))
	cache.Set("b", ByteView("2"))

	// 更新 "a"
	cache.Set("a", ByteView("1-updated"))

	// 缓存顺序：a (最新), b (最久)
	cache.Set("c", ByteView("3")) // 应该淘汰 b

	if _, ok := cache.Get("b"); ok {
		t.Errorf("b should be evicted after update")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Errorf("a should exist")
	}
}

func TestLRUCacheEviction(t *testing.T) {
	cache := NewSimpleLRUCache(2)

	cache.Set("a", ByteView("1"))
	cache.Set("b", ByteView("2"))

	// 访问 a，使 b 成为最久未使用
	cache.Get("a")

	// 添加 c，淘汰 b
	cache.Set("c", ByteView("3"))

	if _, ok := cache.Get("a"); !ok {
		t.Errorf("a should exist")
	}
	if _, ok := cache.Get("b"); ok {
		t.Errorf("b should be evicted")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Errorf("c should exist")
	}
}
