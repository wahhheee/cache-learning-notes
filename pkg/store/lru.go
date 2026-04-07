package store

import (
	"container/list"
	"log/slog"
	"sync"
	"time"
)

// LruCache 基于双向链表的 LRU 缓存实现
type LruCache struct {
	mu      sync.RWMutex
	list    *list.List               // 便于实现淘汰机制
	items   map[string]*list.Element // 对链表节点的映射，实际上的储存结构
	expires map[string]time.Time     // 过期时间

	maxBytes  int64                         // 最大字节
	usedBytes int64                         // 已用字节
	onEvicted func(key string, value Value) // 淘汰回调，可为空

	cleanupInterval time.Duration // 清理间隔
	cleanupTicker   *time.Ticker  // 定时器
	closeCh         chan struct{} // 关闭信号
}

type lruEntry struct {
	key   string
	value Value
}

func newLRUCache(opts Options) *LruCache {
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = 1 * time.Minute
	}

	cache := &LruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: opts.CleanupInterval,
		closeCh:         make(chan struct{}), // 初始化 closeCh 通道
	}

	cache.cleanupTicker = time.NewTicker(opts.CleanupInterval)

	// 开启清理协程
	go cache.cleanupLoop()
	return cache
}

func (c *LruCache) SetWithExpiration(key string, value Value, expiration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	if elem, ok := c.items[key]; ok {
		old := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - old.value.Len())
		old.value = value
		c.list.MoveToFront(elem)
		c.evict()
		return
	}

	newEntry := &lruEntry{key, value}
	elem := c.list.PushFront(newEntry)
	c.items[key] = elem
	c.usedBytes += int64(len(key) + value.Len())
	c.evict()
}

// Get 根据 key 返回 value
func (c *LruCache) Get(key string) (Value, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}

	// 检查是否过期
	c.mu.RLock()
	expTime, hasExp := c.expires[key]
	c.mu.RUnlock()

	if hasExp && time.Now().After(expTime) {
		c.Delete(key) // 直接调用 Delete 方法
		return nil, false
	}

	// 之所以有两段锁，是为了降低写锁的持用时间
	c.mu.Lock()

	// 由于在多线程环境下存在其他 goroutine 删除了 key 的可能性
	// 所以需要进行二次检查
	if _, exits := c.items[key]; exits {
		c.list.MoveToFront(elem)
	}
	c.mu.Unlock()

	return elem.Value.(*lruEntry).value, true
}

// Set 设置键值对
func (c *LruCache) Set(key string, value Value) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 若 key 已存在则更新缓存
	if elem, ok := c.items[key]; ok {
		old := elem.Value.(*lruEntry)
		c.usedBytes -= int64(len(key) + old.value.Len()) // 减去旧值的大小
		c.usedBytes += int64(len(key) + value.Len())     // 加上新值的大小
		old.value = value
		c.list.MoveToFront(elem)

		c.evict()
		return
	}

	newEntry := &lruEntry{key, value}
	elem := c.list.PushFront(newEntry)
	c.items[key] = elem

	// len 对于字符串返回的即是字节数
	c.usedBytes += int64(len(key) + value.Len())

	c.evict()
}

func (c *LruCache) Delete(key string) bool {
	// 持用这么大粒度的写锁
	// 原因是查和删得是原子操作
	// 不进行两段锁检查是考虑到修改的问题
	// 在语意上删除的 key 被中途被修改了这可能是不符合调用方期望的
	// 为了减轻逻辑这里选择持用一个涵盖整个作用域的写锁
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return false
	}

	c.removeElement(elem)
	return true
}

// removeElement 调用前需持用锁
func (c *LruCache) removeElement(elem *list.Element) {
	c.list.Remove(elem)
	// list 和 map 持用的都是指针，实际的内存由 GC 管理
	// 即使在 list 或 map 删除了只要还持用引用就可访问
	e := elem.Value.(*lruEntry)
	delete(c.items, e.key)
	delete(c.expires, e.key)
	c.usedBytes -= int64(len(e.key) + e.value.Len())
	if c.onEvicted != nil {
		c.onEvicted(e.key, e.value)
	}
}

// evict 调用前须持用锁
func (c *LruCache) evict() {
	if c.maxBytes > 0 { // 只有当 maxBytes 大于 0 时才进行淘汰操作
		for c.usedBytes > c.maxBytes && c.list.Len() > 0 {
			elem := c.list.Back()
			if elem != nil {
				e := elem.Value.(*lruEntry)
				c.usedBytes -= int64(len(e.key) + e.value.Len()) // 减去被移除条目的大小
				c.removeElement(elem)
			}
		}
	}
}

func (c *LruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.cleanupExpired()
			c.mu.Unlock()
		case <-c.closeCh:
			slog.Info("Shutdown cleanupLoop...")
			return
		}
	}
}

// cleanupExpired 调用前需持用锁
func (c *LruCache) cleanupExpired() {
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}
}

func (c *LruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

func (c *LruCache) Len() int64 {
	return int64(c.list.Len()) // 返回缓存中实际的条目数
}

func (c *LruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list = list.New()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}
