package store

import (
	"container/list"
)

// LRUCache 简单的 LRU 实现，不考虑多线程下的安全
type LRUCache struct {
	capacity int
	list     *list.List
	items    map[string]*list.Element
}

type entry struct {
	key   string
	value ByteView
}

// NewSimpleLRUCache 构建一个建议 LRU 缓存实例
func NewSimpleLRUCache(cap int) *LRUCache {
	return &LRUCache{
		capacity: cap,
		list:     list.New(),
		items:    make(map[string]*list.Element),
	}
}

// Get 返回只读视图，如果 key 不存在则返回 nil 和 false
func (c *LRUCache) Get(key string) (ByteView, bool) {
	elem, ok := c.items[key]
	if !ok {
		return ByteView{}, false
	}
	c.list.MoveToFront(elem)
	e := elem.Value.(*entry) // 正确地转换为 *entry
	return e.value, true
}

func (c *LRUCache) Set(key string, value ByteView) {
	// 若 key 已存在则更新并移至头部
	if elem, ok := c.items[key]; ok {
		elem.Value = &entry{key, value} // 更新值
		c.list.MoveToFront(elem)
		return
	}

	newElem := c.list.PushFront(&entry{key, value})
	c.items[key] = newElem

	// 超出容量则移除最久未使用的
	if c.list.Len() > c.capacity {
		oldest := c.list.Back()
		if oldest != nil {
			e := oldest.Value.(*entry) // 正确地转换为 *entry
			delete(c.items, e.key)
			c.list.Remove(oldest)
		}
	}
}

func (c *LRUCache) Len() int {
	return c.list.Len()
}
