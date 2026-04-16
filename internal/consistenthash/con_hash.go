package consistenthash

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cache/internal/registry"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Map 是一致性哈希的核心结构。
//
// 功能职责包括：
//   - 在本地维护哈希环。
//   - 管理真实节点及其虚拟节点数量。
//   - 统计本节点及全局节点负载。
//   - 通过 etcd 进行 leader 选举。
//   - 由 leader 负责重平衡并同步哈希环。
//   - 由 follower 监听 leader 同步的哈希环快照。
//
// 并发安全性：
//   - 哈希环结构通过 mu 保护。
//   - selfLoad 通过原子操作统计。
//   - 读操作通常使用读锁，写操作使用写锁。
type Map struct {
	// mu 保护本地哈希环和节点副本信息。
	mu sync.RWMutex

	// config 保存当前实例的配置。
	config *Config

	// keys 保存哈希环上所有虚拟节点的哈希值，并始终保持升序。
	//
	// Get 操作会在该切片上执行二分查找，定位顺时针第一个命中点。
	keys []int

	// hashMap 保存“虚拟节点哈希值 -> 真实节点地址”的映射。
	//
	// keys 负责有序定位，hashMap 负责从环上位置反查真实节点。
	hashMap map[int]string

	// nodeReplicas 保存“真实节点地址 -> 当前虚拟节点数量”的映射。
	//
	// 该字段既用于删除节点时回收对应虚拟节点，也用于重平衡时调整副本数。
	nodeReplicas map[string]int

	// selfLoad 是当前节点本地统计到的负载计数。
	//
	// 当前实现中，当 Get(key) 的路由结果命中 selfAddr 时，selfLoad 加一。
	selfLoad int64

	// nodeCounts 保存当前已知的全局节点负载统计。
	//
	// leader 会通过 etcd 拉取并 watch 所有节点上报的负载值，
	// 然后基于该字段做重平衡判断。
	nodeCounts map[string]int64

	// selfAddr 是当前实例代表的真实节点地址。
	selfAddr string

	// etcdCli 是与 etcd 交互的客户端。
	etcdCli *clientv3.Client

	// ctx/cancel 控制当前实例生命周期。
	//
	// Close 时会触发 cancel，相关后台 goroutine 随后退出。
	ctx    context.Context
	cancel context.CancelFunc
}

// HashRing 表示用于序列化和同步的哈希环快照。
//
// follower 节点通过监听 etcd 中的该结构，直接替换本地哈希环。
type HashRing struct {
	// Keys 是环上所有虚拟节点位置的有序切片。
	Keys []int

	// HashMap 是“虚拟节点位置 -> 真实节点地址”的映射。
	HashMap map[int]string
}

// NewConsistentHash 创建并初始化一个一致性哈希实例。
//
// 初始化过程包括：
//   - 创建本地 Map 结构及默认配置。
//   - 应用可选项。
//   - 连接 etcd。
//   - 启动定时负载上报 goroutine。
//   - 启动选主 goroutine。
//
// 参数：
//   - selfAddr：当前实例代表的节点地址。
//   - opts：可选配置项。
func NewConsistentHash(selfAddr string, opts ...Option) *Map {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
		selfLoad:     0,
		ctx:          ctx,
		cancel:       cancel,
		selfAddr:     selfAddr,
		etcdCli:      nil,
		keys:         make([]int, 0),
		mu:           sync.RWMutex{},
	}

	// 依次应用外部可选配置。
	for _, opt := range opts {
		opt(m)
	}

	// 创建 etcd 客户端。
	//
	// 当前实现固定连接 localhost:2379。
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logrus.Errorf("failed to create etcd client: %v", err)
	}
	m.etcdCli = etcdCli

	// 周期性将当前节点本地负载上报到 etcd。
	//
	// 上报路径格式：
	//   /conhashNodeCount/{selfAddr}
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 上报最近一个统计窗口内的增量负载，并在本地原子清零。
				current := atomic.SwapInt64(&m.selfLoad, 0)
				if err := registry.PutEtcdConHashNodeCount(m.etcdCli, m.selfAddr, current); err != nil {
					logrus.Errorf("failed to report node count for %s: %v", m.selfAddr, err)
				}
			}
		}
	}()

	// 启动 etcd 选主逻辑。
	//
	// 成为 leader 后负责：
	//   - 汇总节点负载；
	//   - 检查并执行重平衡；
	//   - 同步最新哈希环。
	//
	// 竞选失败则作为 follower：
	//   - 监听 leader 变化；
	//   - 监听 leader 发布的哈希环快照。
	go m.RunElection(ctx)

	return m
}

// RunElection 执行基于 etcd 的分布式选主。
//
// leader 的职责：
//   - 汇总全局节点负载；
//   - 周期性检查是否需要重平衡；
//   - 将最新哈希环同步到 etcd。
//
// follower 的职责：
//   - 观察 leader 变化；
//   - 监听 leader 发布的哈希环快照并更新本地状态。
//
// 当 leader 的 etcd session 失效时，会主动结束当前 leader 生命周期，随后重新参与选主。
func (m *Map) RunElection(ctx context.Context) error {
	for {
		session, err := concurrency.NewSession(m.etcdCli)
		if err != nil {
			return err
		}

		election := concurrency.NewElection(session, "/consistenthash/leader")
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)

		// 发起竞选。
		err = election.Campaign(cctx, m.selfAddr)
		if err == nil {
			// 成为 leader 后，创建 leader 生命周期上下文。
			leaderCtx, leaderCancel := context.WithCancel(ctx)

			// session 失效或外部 ctx 结束时，leader 主动退休。
			go func() {
				select {
				case <-session.Done():
					leaderCancel()
				case <-ctx.Done():
					leaderCancel()
				}
			}()

			// leader 负责汇总各节点负载统计。
			go m.updateNodeCount(leaderCtx)

			// leader 负责周期性检查重平衡并同步哈希环。
			go m.CheckAndRebalanceAndSyncHashRing(leaderCtx)

			cancel()

			// 等待 leader 生命周期结束后重新进入选主循环。
			<-leaderCtx.Done()
			continue
		} else {
			// 竞选失败，作为 follower 监听 leader 变化。
			cctx, cancel = context.WithCancel(ctx)
			ch := election.Observe(cctx)

			// follower 同步 leader 发布的哈希环。
			go m.updateHashRing(cctx)

			for {
				select {
				case <-ctx.Done():
					cancel()
					return ctx.Err()
				case resp, ok := <-ch:
					if !ok {
						time.Sleep(time.Second * 2)
						cancel()
						break
					}
					fmt.Println("新 leader:", string(resp.Kvs[0].Value))
				}
			}
		}
	}
}

// Option 定义 Map 的可选配置函数。
//
// 该模式用于在 NewConsistentHash 时按需覆盖默认配置。
type Option func(*Map)

// updateNodeCount 启动 leader 侧的节点负载同步流程。
//
// 处理方式分为两步：
//   - 先从 etcd 全量拉取一次；
//   - 再启动 watch 做增量更新。
func (m *Map) updateNodeCount(ctx context.Context) error {
	// 先全量同步已有负载值。
	m.fetchAllNodeCount(ctx)

	// 再启动增量监听。
	go m.watchNodeCount(ctx)
	return nil
}

// fetchAllNodeCount 从 etcd 全量拉取当前所有节点负载。
//
// etcd 键格式：
//
//	/conhashNodeCount/{addr} = count
func (m *Map) fetchAllNodeCount(ctx context.Context) error {
	resp, err := m.etcdCli.Get(ctx, "/conhashNodeCount/", clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		addr := strings.TrimPrefix(string(kv.Key), "/conhashNodeCount/")
		count, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return err
		}
		m.nodeCounts[addr] = count
	}
	return nil
}

// watchNodeCount 监听 etcd 中节点负载的增量变化。
//
// 当某个节点更新自己的负载时，leader 会同步刷新本地 nodeCounts。
func (m *Map) watchNodeCount(ctx context.Context) error {
	watchCh := m.etcdCli.Watch(ctx, "/conhashNodeCount/", clientv3.WithPrefix())
	for resp := range watchCh {
		for _, event := range resp.Events {
			addr := strings.TrimPrefix(string(event.Kv.Key), "/conhashNodeCount/")
			count, err := strconv.ParseInt(string(event.Kv.Value), 10, 64)
			if err != nil {
				return err
			}
			m.nodeCounts[addr] = count
		}
	}
	return nil
}

// CheckAndRebalanceAndSyncHashRing 是 leader 的周期性重平衡任务。
//
// 执行逻辑：
//   - 每 10 秒检查一次 nodeCounts。
//   - 若负载分布超出阈值，则调整副本数。
//   - 若发生了副本调整，则同步最新哈希环到 etcd。
func (m *Map) CheckAndRebalanceAndSyncHashRing(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err, IsRebalance := m.checkAndRebalance()
			if err != nil {
				logrus.Errorf("failed to check and rebalance: %v", err)
			}
			if IsRebalance {
				m.syncHashRing()
			}
		}
	}
}

// updateHashRing 监听 leader 发布到 etcd 的哈希环快照，并更新本地环。
//
// follower 通过该方法获得 leader 当前维护的官方哈希环版本。
func (m *Map) updateHashRing(ctx context.Context) error {
	watchCh := m.etcdCli.Watch(ctx, "/consistenthash/hashring", clientv3.WithPrefix())
	for resp := range watchCh {
		for _, event := range resp.Events {
			hashRing := HashRing{}
			if err := json.Unmarshal(event.Kv.Value, &hashRing); err != nil {
				logrus.Errorf("failed to unmarshal hashring: %v", err)
				continue
			}

			m.mu.Lock()
			m.keys = hashRing.Keys
			m.hashMap = hashRing.HashMap
			m.mu.Unlock()
		}
	}
	return nil
}

// syncHashRing 将当前哈希环快照同步到 etcd。
//
// 同步内容包括：
//   - keys
//   - hashMap
//
// 同步后会将所有 nodeCounts 重置为 0，并回写 etcd，表示新一轮负载统计开始。
func (m *Map) syncHashRing() error {
	hashRing := HashRing{
		Keys:    m.keys,
		HashMap: m.hashMap,
	}
	hashRingData, err := json.Marshal(hashRing)
	if err != nil {
		return err
	}

	_, err = m.etcdCli.Put(context.Background(), "/consistenthash/hashring", string(hashRingData))
	if err != nil {
		return err
	}

	return nil
}

// Add 向哈希环中加入一个真实节点。
//
// 若节点已存在，则直接返回。
// 若节点不存在，则按默认副本数创建虚拟节点并加入哈希环。
func (m *Map) Add(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.nodeReplicas[addr]; ok {
		return nil
	}

	replicas := m.config.DefaultReplicas
	err := m.addNode(addr, replicas)
	if err != nil {
		return err
	}
	return nil
}

// addNode 在不额外加锁的前提下，将一个真实节点加入哈希环。
//
// 具体做法：
//   - 生成 addr-0、addr-1、...、addr-(replicas-1) 形式的虚拟节点标识；
//   - 对每个虚拟节点标识做哈希，得到环上的位置；
//   - 将所有位置写入 keys 和 hashMap；
//   - 记录节点副本数并重新排序 keys。
func (m *Map) addNode(addr string, replicas int) error {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", addr, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = addr
		m.nodeCounts[addr] = 0
	}
	m.nodeReplicas[addr] = replicas

	sort.Ints(m.keys)
	return nil
}

// Remove 从哈希环中移除一个真实节点。
//
// 若节点不存在，则直接返回。
// 若节点存在，则删除其全部虚拟节点，并移除相关统计信息。
func (m *Map) Remove(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.nodeReplicas[addr]; !ok {
		return nil
	}

	err := m.removeNoLock(addr)
	if err != nil {
		return err
	}
	return nil
}

// removeNoLock 在不额外加锁的前提下，从哈希环中删除一个真实节点。
//
// 删除流程：
//   - 根据 nodeReplicas[addr] 获取虚拟节点数量；
//   - 按固定命名规则重建每个虚拟节点的哈希值；
//   - 从 hashMap 和 keys 中逐一移除；
//   - 最后删除该节点的副本信息和负载统计。
func (m *Map) removeNoLock(addr string) error {
	replicas := m.nodeReplicas[addr]
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", addr, i))))
		delete(m.hashMap, hash)
		m.keys = slice_remove(m.keys, hash)
	}
	delete(m.nodeReplicas, addr)
	delete(m.nodeCounts, addr)
	return nil
}

// slice_remove 从有序切片中删除指定值。
//
// 删除策略：
//   - 先通过二分查找定位目标值；
//   - 若存在则返回删除后的新切片；
//   - 若不存在则返回原切片。
func slice_remove(slice []int, value int) []int {
	index := sort.SearchInts(slice, value)
	if index < len(slice) && slice[index] == value {
		return append(slice[:index], slice[index+1:]...)
	}
	return slice
}

// Get 返回 key 在当前哈希环上对应的真实节点地址。
//
// 查找流程：
//   - 对业务 key 做哈希；
//   - 在有序虚拟节点数组 keys 中二分查找第一个 >= hash 的位置；
//   - 若未找到，则回绕到 keys[0]；
//   - 通过 hashMap 返回对应真实节点地址。
//
// 附加逻辑：
//   - 若最终选中的是 selfAddr，则将 selfLoad 原子加一。
func (m *Map) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.config.HashFunc([]byte(key)))

	// 二分查找顺时针第一个大于等于 hash 的虚拟节点位置。
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 若 hash 大于环上所有点，则回绕到环的起点。
	if index == len(m.keys) {
		index = 0
	}

	// 若命中当前实例自身，则累计本地负载统计。
	if m.hashMap[m.keys[index]] == m.selfAddr {
		atomic.AddInt64(&m.selfLoad, 1)
	}

	return m.hashMap[m.keys[index]]
}

// checkAndRebalance 根据当前 nodeCounts 判断是否需要重平衡。
//
// 核心策略：
//   - 先计算平均负载 avg；
//   - 若某节点负载 / avg > MaxLoadBalanceThreshold，则减少其副本数；
//   - 若某节点负载 / avg < MinLoadBalanceThreshold，则增加其副本数；
//   - 调整步长固定为 10；
//   - 调整结果受 MinReplicas 和 MaxReplicas 约束。
//
// 返回值：
//   - error：执行重平衡过程中遇到的错误；
//   - bool：是否实际发生了副本调整。
func (m *Map) checkAndRebalance() (error, bool) {
	total := int64(0)
	for _, val := range m.nodeCounts {
		total += val
	}

	IsRebalance := false
	avg := total / int64(len(m.nodeCounts))
	if avg == 0 {
		return nil, IsRebalance
	}

	for addr, val := range m.nodeCounts {
		newReplicas := m.nodeReplicas[addr]
		flag := false

		if float64(val)/float64(avg) > m.config.MaxLoadBalanceThreshold {
			newReplicas = m.nodeReplicas[addr] - 10
			flag = true
		} else if float64(val)/float64(avg) < m.config.MinLoadBalanceThreshold {
			newReplicas = m.nodeReplicas[addr] + 10
			flag = true
		}

		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if flag {
			IsRebalance = true
			err := m.rebalanceReplicas(addr, newReplicas)
			if err != nil {
				logrus.Errorf("failed to rebalance replicas for %s: %v", addr, err)
				return err, IsRebalance
			}
		}
	}
	return nil, IsRebalance
}

// rebalanceReplicas 将指定节点的副本数调整为 newReplicas。
//
// 当前实现采用“先删后加”的方式：
//   - 先移除该节点现有全部虚拟节点；
//   - 再按新的副本数重新加入。
func (m *Map) rebalanceReplicas(addr string, newReplicas int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.removeNoLock(addr)
	if err != nil {
		return err
	}
	err = m.addNode(addr, newReplicas)
	if err != nil {
		return err
	}
	return nil
}

// Close 关闭当前一致性哈希实例。
//
// 关闭动作包括：
//   - 触发 cancel，通知后台 goroutine 退出；
//   - 关闭 etcd 客户端。
func (m *Map) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.etcdCli != nil {
		m.etcdCli.Close()
	}
	return nil
}
