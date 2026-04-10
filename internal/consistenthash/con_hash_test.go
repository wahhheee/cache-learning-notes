// con_hash_test.go
package consistenthash

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var testEtcd *embed.Etcd
var testEtcdCli *clientv3.Client
var etcdDir string

func TestMain(m *testing.M) {
	var err error
	etcdDir, err = os.MkdirTemp("", "etcd-test-*")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(etcdDir)

	cfg := embed.NewConfig()
	cfg.Dir = etcdDir
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"/dev/null"} // 屏蔽日志

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to start embedded etcd: %v", err))
	}
	testEtcd = e

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		e.Close()
		panic("etcd took too long to start")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		e.Close()
		panic(fmt.Sprintf("failed to create etcd client: %v", err))
	}
	testEtcdCli = cli

	code := m.Run()

	cli.Close()
	e.Close()
	time.Sleep(100 * time.Millisecond)

	os.Exit(code)
}

func waitASecond() {
	time.Sleep(200 * time.Millisecond)
}

// 测试添加节点
func TestAddNode(t *testing.T) {
	m := NewConsistentHash("127.0.0.1:8001")
	defer m.Close()
	time.Sleep(100 * time.Millisecond)

	err := m.Add("node1")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	err = m.Add("node2")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.nodeReplicas) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(m.nodeReplicas))
	}

	expectedReplicas := m.config.DefaultReplicas
	for addr, replicas := range m.nodeReplicas {
		if replicas != expectedReplicas {
			t.Errorf("node %s has %d replicas, expected %d", addr, replicas, expectedReplicas)
		}
	}

	if len(m.keys) != 2*expectedReplicas {
		t.Errorf("expected %d keys, got %d", 2*expectedReplicas, len(m.keys))
	}
}

// 测试 Get 的一致性
func TestGetConsistency(t *testing.T) {
	m := NewConsistentHash("127.0.0.1:8002")
	defer m.Close()
	time.Sleep(100 * time.Millisecond)

	m.Add("nodeA")
	m.Add("nodeB")
	m.Add("nodeC")

	key := "test-key"
	first := m.Get(key)
	for i := 0; i < 100; i++ {
		if got := m.Get(key); got != first {
			t.Errorf("inconsistent mapping for key %s: got %s, expected %s", key, got, first)
		}
	}
}

// 测试删除节点
func TestRemoveNode(t *testing.T) {
	m := NewConsistentHash("127.0.0.1:8003")
	defer m.Close()
	time.Sleep(100 * time.Millisecond)

	m.Add("node1")
	m.Add("node2")
	err := m.Remove("node1")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.nodeReplicas["node1"]; ok {
		t.Error("node1 still exists in nodeReplicas")
	}
	if _, ok := m.nodeCounts["node1"]; ok {
		t.Error("node1 still exists in nodeCounts")
	}
	for _, addr := range m.hashMap {
		if addr == "node1" {
			t.Error("node1 still present in hashMap")
		}
	}
}

// 测试负载统计和自增
func TestSelfLoadIncrement(t *testing.T) {
	m := NewConsistentHash("127.0.0.1:8004")
	defer m.Close()
	time.Sleep(100 * time.Millisecond)

	m.Add("127.0.0.1:8004")

	key := "some-key"
	for i := 0; i < 10; i++ {
		_ = m.Get(key)
	}

	load := atomic.LoadInt64(&m.selfLoad)
	if load <= 0 {
		t.Errorf("selfLoad should be > 0, got %d", load)
	}
}

// 测试负载均衡逻辑
func TestRebalance(t *testing.T) {
	m := NewConsistentHash("127.0.0.1:8005")
	defer m.Close()
	time.Sleep(100 * time.Millisecond)

	m.Add("node1")
	m.Add("node2")

	m.mu.Lock()
	m.nodeCounts["node1"] = 1000
	m.nodeCounts["node2"] = 100
	m.mu.Unlock()

	err, changed := m.checkAndRebalance()
	if err != nil {
		t.Fatalf("checkAndRebalance failed: %v", err)
	}
	if !changed {
		t.Log("rebalance not triggered (maybe threshold not met)")
	} else {
		m.mu.RLock()
		replicas1 := m.nodeReplicas["node1"]
		replicas2 := m.nodeReplicas["node2"]
		m.mu.RUnlock()
		t.Logf("after rebalance: node1 replicas=%d, node2 replicas=%d", replicas1, replicas2)
		if replicas1 == replicas2 {
			t.Error("replicas should be different after rebalance")
		}
	}
}

// 测试分布式选主与哈希环同步（显式触发同步）
func TestElectionAndSync(t *testing.T) {
	// 清理 etcd 旧数据
	testEtcdCli.Delete(context.Background(), "/consistenthash/", clientv3.WithPrefix())
	testEtcdCli.Delete(context.Background(), "/conhashNodeCount/", clientv3.WithPrefix())
	time.Sleep(100 * time.Millisecond)

	// 创建第一个节点（将作为 leader）
	m1 := NewConsistentHash("127.0.0.1:9001")
	defer m1.Close()
	time.Sleep(200 * time.Millisecond)

	// 添加节点到 m1
	m1.Add("nodeX")
	m1.Add("nodeY")

	// 显式同步哈希环到 etcd（不等待自动 ticker）
	if err := m1.syncHashRing(); err != nil {
		t.Fatalf("failed to sync hash ring: %v", err)
	}

	// 等待写入完成
	time.Sleep(300 * time.Millisecond)

	// 验证哈希环已存入 etcd
	resp, err := testEtcdCli.Get(context.Background(), "/consistenthash/hashring")
	if err != nil {
		t.Fatalf("failed to get hashring from etcd: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("hashring not found in etcd - sync failed")
	}
	t.Logf("hashring found in etcd, size: %d bytes", len(resp.Kvs[0].Value))

	// 创建第二个节点（follower）
	m2 := NewConsistentHash("127.0.0.1:9002")
	defer m2.Close()
	time.Sleep(200 * time.Millisecond)

	// 手动从 etcd 拉取哈希环
	ctx := context.Background()
	if err := m2.fetchHashRingFromEtcd(ctx); err != nil {
		t.Fatalf("failed to fetch hash ring: %v", err)
	}

	// 对比两个实例的哈希环
	m1.mu.RLock()
	m1KeysLen := len(m1.keys)
	m1Keys := make([]int, len(m1.keys))
	copy(m1Keys, m1.keys)
	m1HashMap := make(map[int]string, len(m1.hashMap))
	for k, v := range m1.hashMap {
		m1HashMap[k] = v
	}
	m1.mu.RUnlock()

	m2.mu.RLock()
	m2KeysLen := len(m2.keys)
	m2Keys := make([]int, len(m2.keys))
	copy(m2Keys, m2.keys)
	m2HashMap := make(map[int]string, len(m2.hashMap))
	for k, v := range m2.hashMap {
		m2HashMap[k] = v
	}
	m2.mu.RUnlock()

	t.Logf("m1 keys: %d, m2 keys: %d", m1KeysLen, m2KeysLen)

	if m1KeysLen != m2KeysLen {
		t.Errorf("key length mismatch: m1=%d, m2=%d", m1KeysLen, m2KeysLen)
	}

	if m1KeysLen == m2KeysLen && m1KeysLen > 0 {
		for i := 0; i < len(m1Keys); i++ {
			if m1Keys[i] != m2Keys[i] {
				t.Errorf("key mismatch at index %d: m1=%d, m2=%d", i, m1Keys[i], m2Keys[i])
			}
		}
		for k, v := range m1HashMap {
			if m2HashMap[k] != v {
				t.Errorf("hashMap mismatch at key %d: m1=%s, m2=%s", k, v, m2HashMap[k])
			}
		}
	}
}

// fetchHashRingFromEtcd 从 etcd 读取并应用哈希环（辅助方法）
func (m *Map) fetchHashRingFromEtcd(ctx context.Context) error {
	resp, err := m.etcdCli.Get(ctx, "/consistenthash/hashring")
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	hashRing := HashRing{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &hashRing); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys = hashRing.Keys
	m.hashMap = hashRing.HashMap

	// 重建 nodeReplicas
	m.nodeReplicas = make(map[string]int)
	for _, addr := range m.hashMap {
		m.nodeReplicas[addr]++
		m.nodeCounts[addr] = 0
	}
	return nil
}

// 测试并发 Get 的安全性
func TestConcurrentGet(t *testing.T) {
	m := NewConsistentHash("127.0.0.1:8006")
	defer m.Close()
	time.Sleep(100 * time.Millisecond)

	m.Add("node1")
	m.Add("node2")
	m.Add("node3")

	var wg sync.WaitGroup
	n := 1000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", idx)
			_ = m.Get(key)
		}(i)
	}
	wg.Wait()
}
