package store

import (
	"sync"
	"sync/atomic"
	"time"
)

// lru2Store 是一个分桶的两级缓存实现。
//
// 结构特征：
//   - 外层按 key 的哈希值分桶，每个桶使用独立互斥锁，降低并发竞争。
//   - 每个桶内部包含两层 cache：
//   - caches[i][0]：一级缓存，用于接收新写入的数据。
//   - caches[i][1]：二级缓存，用于保存从一级淘汰或从一级晋升的数据。
//   - key 的读写、删除、过期处理都只发生在所属桶内。
type lru2Store struct {
	// locks 为每个桶提供一把独立互斥锁。
	locks []sync.Mutex

	// caches 保存所有桶的两级缓存。
	// 第二维固定为 2：
	//   [0] 为一级缓存；
	//   [1] 为二级缓存。
	caches [][2]*cache

	// onEvicted 在项目最终淘汰时触发。
	// 一级缓存内部的替换不会直接触发该回调；
	// 一级被替换的节点会先迁移到二级，由二级容量淘汰时再触发回调。
	onEvicted func(key string, value Value)

	// cleanupTick 驱动后台过期清理。
	cleanupTick *time.Ticker

	// mask 用于将哈希值映射到桶下标。
	// 桶数量会被扩展为 2 的幂，因此可以通过按位与高效取模：
	//   idx = hashBKRD(key) & mask
	mask int32
}

// newLRU2Cache 创建并初始化 lru2Store。
//
// 初始化流程：
//   - 补齐默认配置。
//   - 根据 BucketCount 计算桶掩码。
//   - 为每个桶分配一级缓存、二级缓存和独立锁。
//   - 启动后台过期清理协程。
func newLRU2Cache(opts Options) *lru2Store {
	if opts.BucketCount == 0 {
		opts.BucketCount = 16
	}
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024
	}
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}

	// mask 是“大于等于 BucketCount 的最近 2 的幂 - 1”。
	// 假设 BucketCount=16，则 mask=15，合法下标范围为 [0, 15]。
	mask := maskOfNextPowOf2(opts.BucketCount)
	s := &lru2Store{
		locks:       make([]sync.Mutex, mask+1),
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTick: time.NewTicker(opts.CleanupInterval),
		mask:        int32(mask),
	}

	// 为每个桶分配两层 cache。
	for i := range s.caches {
		s.caches[i][0] = Create(opts.CapPerBucket)
		s.caches[i][1] = Create(opts.Level2Cap)
	}

	// 启动后台清理协程。
	if opts.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// Get 获取 key 对应的值。
//
// 读取流程：
//   - 先根据 key 定位到所属桶，并锁定该桶。
//   - 优先检查一级缓存：
//   - 若命中且未过期，则从一级删除后写入二级，并返回值。
//   - 若命中但已过期，则统一删除并返回未命中。
//   - 一级未命中时，再检查二级缓存：
//   - 若命中且未过期，则直接返回值。
//   - 若命中但已过期，则统一删除并返回未命中。
//
// 设计含义：
//   - 一级用于接收新写入数据。
//   - 从一级命中后会晋升到二级。
//   - 二级用于保留更“稳定”的热点数据。
func (s *lru2Store) Get(key string) (Value, bool) {
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	currentTime := Now()

	// 优先检查一级缓存。
	//
	// 这里调用 del 而不是 get，原因是一级命中后需要将节点移出一级，
	// 并晋升到二级缓存。
	n1, status1, expireAt := s.caches[idx][0].del(key)
	if status1 > 0 {
		// 一级命中后先判断过期。
		if expireAt > 0 && currentTime >= expireAt {
			// 已过期的数据统一通过 delete 清理两层状态。
			s.delete(key, idx)
			return nil, false
		}

		// 未过期，晋升到二级缓存。
		s.caches[idx][1].put(key, n1.v, expireAt, s.onEvicted)
		return n1.v, true
	}

	// 一级未命中时检查二级缓存。
	n2, status2 := s._get(key, idx, 1)
	if status2 > 0 && n2 != nil {
		// _get 已经做过过期检查。
		// 这里保留一次显式判断，便于维持当前逻辑完整性。
		if n2.expireAt > 0 && currentTime >= n2.expireAt {
			s.delete(key, idx)
			return nil, false
		}

		return n2.v, true
	}

	return nil, false
}

// Set 写入一个近似“长期有效”的值。
//
// 当前实现通过传入一个很大的 Duration 来模拟长期有效。
// 实际仍是带过期时间的写入，而非真正的“永不过期”。
func (s *lru2Store) Set(key string, value Value) error {
	return s.SetWithExpiration(key, value, 9999999999999999)
}

// SetWithExpiration 写入带过期时间的值。
//
// 写入流程：
//   - 计算绝对过期时间 expireAt。
//   - 根据 key 哈希定位到所属桶，并锁定该桶。
//   - 将新值写入一级缓存。
//   - 若一级缓存因为写入发生替换，则将被替换的尾节点迁移到二级缓存。
//   - 二级缓存满时，才通过 onEvicted 触发最终淘汰回调。
//
// 当前实现采用“一级接收写入、一级淘汰迁移至二级”的策略。
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// expireAt 使用绝对纳秒时间戳。
	// expiration <= 0 时，expireAt 保持为 0。
	expireAt := int64(0)
	if expiration > 0 {
		expireAt = Now() + int64(expiration.Nanoseconds())
	}

	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	l1 := s.caches[idx][0]
	var evicted *node

	// 仅在“新增写入且一级已满”时，记录一级尾节点。
	//
	// 该尾节点会在 put 中被原地复用，因此需要在写入前先拷贝旧值。
	// 只有仍处于有效状态的节点（expireAt > 0）才会迁移到二级。
	if _, exists := l1.hmap[key]; !exists && l1.last == uint16(cap(l1.m)) {
		tailIdx := l1.dlnk[0][p]
		if tailIdx > 0 {
			tail := l1.m[tailIdx-1]
			if tail.expireAt > 0 {
				evicted = new(node)
				*evicted = tail
			}
		}
	}

	// 写入一级缓存。
	//
	// 一级内部替换不视为最终淘汰，因此这里不传 onEvicted。
	l1.put(key, value, expireAt, nil)

	// 若一级发生有效替换，则将被替换节点迁移至二级。
	// 二级再发生容量淘汰时，才触发 onEvicted。
	if evicted != nil {
		s.caches[idx][1].put(evicted.k, evicted.v, evicted.expireAt, s.onEvicted)
	}

	return nil
}

// Delete 删除指定 key。
//
// 删除操作会同时尝试删除当前桶的一级和二级缓存。
// 只要任意一层存在该 key，即视为删除成功。
func (s *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	return s.delete(key, idx)
}

// Clear 清空整个 store。
//
// 处理流程：
//   - 先遍历所有桶，收集所有有效 key。
//   - 再逐个调用 Delete，复用统一删除逻辑。
//
// 采用“两段式”处理而非边遍历边删除，避免在 walk 期间修改链表结构。
func (s *lru2Store) Clear() {
	var keys []string

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			keys = append(keys, key)
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			// 避免重复收集两层中同名 key。
			for _, k := range keys {
				if key == k {
					return true
				}
			}
			keys = append(keys, key)
			return true
		})

		s.locks[i].Unlock()
	}

	for _, key := range keys {
		s.Delete(key)
	}
}

// Len 返回当前 store 中的有效项数量。
//
// 统计方式：
//   - 遍历每个桶的一级缓存和二级缓存。
//   - 通过 walk 仅统计 expireAt > 0 的有效节点。
//
// 注意：若同一 key 在某一时刻同时存在于一级和二级，计数会累计两次。
func (s *lru2Store) Len() int {
	count := 0

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})

		s.locks[i].Unlock()
	}

	return count
}

// Close 停止后台清理定时器。
func (s *lru2Store) Close() {
	if s.cleanupTick != nil {
		s.cleanupTick.Stop()
	}
}

// clock 是一个近似的内部时间戳缓存，用于降低频繁调用 time.Now 的开销。
// p 和 n 分别表示双向链表中的 prev 与 next 下标位置。
var clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)

// Now 返回内部时钟当前值。
//
// 该函数通过原子读取得到近似当前时间，单位为纳秒时间戳。
func Now() int64 { return atomic.LoadInt64(&clock) }

// init 启动内部时钟维护协程。
//
// 时间维护策略：
//   - 每轮先使用真实时间进行一次校准。
//   - 之后每 100ms 通过原子加法推进 clock。
//   - 周期性重新校准，以平衡精度和开销。
func init() {
	go func() {
		for {
			// 每轮以真实系统时间重新校准。
			atomic.StoreInt64(&clock, time.Now().UnixNano())

			// 之后按固定步长推进，避免高频调用 time.Now。
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond))
			}

			// 保持整轮节奏为约 1 秒。
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// hashBKRD 计算字符串的 BKDR 哈希值。
//
// BKDR 是一种简单的字符串哈希算法，常用于将字符串均匀分散到桶中。
func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// maskOfNextPowOf2 返回“大于等于 cap 的最近 2 的幂 - 1”。
//
// 返回值用于按位于取模，前提是桶数量被规范化到 2 的幂。
// 例如：
//   - cap=16，返回 15
//   - cap=10，返回 15，对应内部按 16 个桶处理
func maskOfNextPowOf2(cap uint16) uint16 {
	// 已经是 2 的幂时，直接返回 cap-1。
	if cap > 0 && cap&(cap-1) == 0 {
		return cap - 1
	}

	// 通过逐步扩展最高位右侧的所有位，构造全 1 掩码。
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
}

// node 是缓存中的逻辑节点。
type node struct {
	// k 为节点对应的 key。
	k string

	// v 为节点对应的 value。
	v Value

	// expireAt 为绝对过期时间戳，单位为纳秒。
	// expireAt == 0 表示逻辑上已删除或无效。
	expireAt int64
}

// cache 是桶内单层缓存的核心结构。
//
// 底层由三部分组成：
//   - hmap：key 到节点编号的映射，用于 O(1) 定位。
//   - m：节点数据数组，按编号存储实际节点。
//   - dlnk：使用数组模拟的双向链表，用于维护 LRU 顺序。
type cache struct {
	// dlnk 是数组模拟双向链表的结构。
	//
	// 约定：
	//   - dlnk[i][p] 表示节点 i 的前驱编号；
	//   - dlnk[i][n] 表示节点 i 的后继编号。
	//
	// 其中 dlnk[0] 是哨兵节点：
	//   - dlnk[0][p] 保存尾节点编号；
	//   - dlnk[0][n] 保存头节点编号。
	dlnk [][2]uint16

	// m 存储实际节点数据。
	// 节点编号 idx 对应到 m[idx-1]。
	m []node

	// hmap 保存 key 到节点编号的映射。
	hmap map[string]uint16

	// last 是当前已分配到的最大节点编号。
	// 编号从 1 开始，0 预留给哨兵节点。
	last uint16
}

// Create 创建一个指定容量的 cache。
//
// 分配策略：
//   - dlnk 长度为 cap+1，其中 0 号位置作为哨兵。
//   - m 长度为 cap，用于存储实际节点。
//   - hmap 预分配 cap 大小，减少扩容次数。
func Create(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, cap+1),
		m:    make([]node, cap),
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

// put 向 cache 中写入一个 key/value。
//
// 返回值：
//   - 1 表示新增；
//   - 0 表示更新。
//
// 行为分为三种情况：
//   - key 已存在：更新值和过期时间，并移动到链表头部。
//   - key 不存在且容量未满：分配新节点并插入链表头部。
//   - key 不存在且容量已满：复用尾节点槽位，将尾节点改写为新节点并移动到头部。
func (c *cache) put(key string, val Value, expireAt int64, onEvicted func(string, Value)) int {
	// 已存在则更新并刷新到头部。
	if idx, ok := c.hmap[key]; ok {
		c.m[idx-1].v, c.m[idx-1].expireAt = val, expireAt
		c.adjust(idx, p, n)
		return 0
	}

	// 容量已满时，直接复用尾节点的物理槽位。
	if c.last == uint16(cap(c.m)) {
		tail := &c.m[c.dlnk[0][p]-1]

		// 仅对有效尾节点触发淘汰回调。
		if onEvicted != nil && (*tail).expireAt > 0 {
			onEvicted((*tail).k, (*tail).v)
		}

		// 删除旧 key 的索引，并将尾槽位改写成新 key。
		delete(c.hmap, (*tail).k)
		c.hmap[key], (*tail).k, (*tail).v, (*tail).expireAt = c.dlnk[0][p], key, val, expireAt

		// 改写完成后，将该节点移动到头部。
		c.adjust(c.dlnk[0][p], p, n)

		return 1
	}

	// 容量未满时，分配一个新编号。
	c.last++

	// 首节点同时成为头和尾。
	if len(c.hmap) <= 0 {
		c.dlnk[0][p] = c.last
	} else {
		// 非首节点插入头部前，先让旧头的前驱指向新节点。
		c.dlnk[c.dlnk[0][n]][p] = c.last
	}

	// 初始化新节点，并将其插入链表头部。
	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt
	c.dlnk[c.last] = [2]uint16{0, c.dlnk[0][n]}
	c.hmap[key] = c.last
	c.dlnk[0][n] = c.last

	return 1
}

// get 从 cache 中获取 key 对应的节点。
//
// 命中时会将节点移动到链表头部，以维护 LRU 顺序。
// 返回值：
//   - (*node, 1) 表示命中；
//   - (nil, 0) 表示未命中。
//
// 该方法不负责判定过期状态，仅负责结构层面的获取与刷新。
func (c *cache) get(key string) (*node, int) {
	if idx, ok := c.hmap[key]; ok {
		c.adjust(idx, p, n)
		return &c.m[idx-1], 1
	}
	return nil, 0
}

// del 删除 cache 中指定 key 的逻辑有效性。
//
// 删除行为不是立即回收物理槽位，而是：
//   - 将 expireAt 设为 0，表示逻辑删除；
//   - 将节点移动到链表尾部，便于后续优先复用尾部槽位。
//
// 返回值：
//   - (*node, 1, oldExpireAt) 表示删除成功；
//   - (nil, 0, 0) 表示未命中或节点已无效。
func (c *cache) del(key string) (*node, int, int64) {
	if idx, ok := c.hmap[key]; ok && c.m[idx-1].expireAt > 0 {
		e := c.m[idx-1].expireAt
		c.m[idx-1].expireAt = 0
		c.adjust(idx, n, p)
		return &c.m[idx-1], 1, e
	}

	return nil, 0, 0
}

// walk 按当前链表顺序遍历所有有效节点。
//
// 遍历顺序为从头到尾。
// 遍历过程中只访问 expireAt > 0 的节点。
// 当 walker 返回 false 时，遍历提前终止。
func (c *cache) walk(walker func(key string, value Value, expireAt int64) bool) {
	for idx := c.dlnk[0][n]; idx != 0; idx = c.dlnk[idx][n] {
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k, c.m[idx-1].v, c.m[idx-1].expireAt) {
			return
		}
	}
}

// adjust 调整节点在双向链表中的位置。
//
// 参数说明：
//   - idx：待移动的节点编号。
//   - f：移动后会被置为 0 的方向。
//   - t：移动后会连接到原头/尾的方向。
//
// 调用约定：
//   - adjust(idx, p, n)：移动到头部。
//   - adjust(idx, n, p)：移动到尾部。
//
// 核心步骤：
//   - 先将节点从原位置摘除；
//   - 再将节点插入目标位置；
//   - 全过程只修改链表索引，不移动实际节点数据。
func (c *cache) adjust(idx, f, t uint16) {
	// 仅当节点不在目标端点时才需要移动。
	if c.dlnk[idx][f] != 0 {
		// 将左右邻居重新连接，完成摘除。
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]

		// 插入到目标端点。
		c.dlnk[idx][f] = 0
		c.dlnk[idx][t] = c.dlnk[0][t]
		c.dlnk[c.dlnk[0][t]][f] = idx
		c.dlnk[0][t] = idx
	}
}

// _get 是 lru2Store 的内部层级读取方法。
//
// 参数：
//   - key：目标键。
//   - idx：桶下标。
//   - level：层级，0 表示一级，1 表示二级。
//
// 行为：
//   - 调用对应层的 get 刷新 LRU 顺序；
//   - 判断节点是否已删除或已过期；
//   - 返回有效节点或未命中状态。
func (s *lru2Store) _get(key string, idx, level int32) (*node, int) {
	if n, st := s.caches[idx][level].get(key); st > 0 && n != nil {
		currentTime := Now()
		if n.expireAt <= 0 || currentTime >= n.expireAt {
			return nil, 0
		}
		return n, st
	}

	return nil, 0
}

// delete 是 lru2Store 的内部统一删除入口。
//
// 删除逻辑：
//   - 同时删除指定桶中的一级和二级。
//   - 任意一层命中即视为删除成功。
//   - 删除成功后，如配置了 onEvicted，则触发一次回调。
func (s *lru2Store) delete(key string, idx int32) bool {
	n1, s1, _ := s.caches[idx][0].del(key)
	n2, s2, _ := s.caches[idx][1].del(key)
	deleted := s1 > 0 || s2 > 0

	if deleted && s.onEvicted != nil {
		if n1 != nil && n1.v != nil {
			s.onEvicted(key, n1.v)
		} else if n2 != nil && n2.v != nil {
			s.onEvicted(key, n2.v)
		}
	}

	return deleted
}

// cleanupLoop 后台周期性清理过期项。
//
// 处理流程：
//   - 每次定时器触发后，读取当前时间。
//   - 逐桶加锁并扫描一级、二级。
//   - 收集已过期的 key。
//   - 统一调用 delete 删除，避免遍历期间直接修改链表。
func (s *lru2Store) cleanupLoop() {
	for range s.cleanupTick.C {
		currentTime := Now()

		for i := range s.caches {
			s.locks[i].Lock()

			// 先收集，后删除，避免 walk 期间修改内部结构。
			var expiredKeys []string

			s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
				if expireAt > 0 && currentTime >= expireAt {
					expiredKeys = append(expiredKeys, key)
				}
				return true
			})

			s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
				if expireAt > 0 && currentTime >= expireAt {
					// 同一 key 可能出现在两层，避免重复加入。
					for _, k := range expiredKeys {
						if key == k {
							return true
						}
					}
					expiredKeys = append(expiredKeys, key)
				}
				return true
			})

			for _, key := range expiredKeys {
				s.delete(key, int32(i))
			}

			s.locks[i].Unlock()
		}
	}
}
