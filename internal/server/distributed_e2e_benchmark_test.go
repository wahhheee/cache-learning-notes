package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cache/internal/database"
	"cache/internal/registry"
	pb "cache/pkg/pb"
	"cache/pkg/store"

	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const benchmarkE2ENodeCount = 3

type benchmarkE2ECluster struct {
	svcName  string
	prefix   string
	servers  []*Server
	clients  []*grpc.ClientConn
	entry    pb.CacheClient
	redisCli *redis.Client
}

func newBenchmarkE2ECluster(tb testing.TB, nodeCount int) *benchmarkE2ECluster {
	tb.Helper()

	logrusStandardSilence()

	database.DefaultConfig.RedisHost = "127.0.0.1"
	registry.DefaultConfig.Endpoints = []string{"127.0.0.1:2379"}

	prefix := fmt.Sprintf("bench:e2e:%d", time.Now().UnixNano())
	svcName := "cache-bench-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	addrs := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		addrs = append(addrs, benchmarkFreeAddr(tb))
	}

	cluster := &benchmarkE2ECluster{
		svcName: svcName,
		prefix:  prefix,
		servers: make([]*Server, 0, nodeCount),
		clients: make([]*grpc.ClientConn, 0, nodeCount),
		redisCli: redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
			DB:   0,
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cluster.redisCli.Ping(ctx).Err(); err != nil {
		tb.Fatalf("redis ping failed: %v", err)
	}

	for _, addr := range addrs {
		srv := NewServer(addr, svcName)
		group := NewGroup(svcName, addr, store.Options{
			BucketCount:     64,
			CapPerBucket:    1024,
			Level2Cap:       1024,
			CleanupInterval: time.Hour,
		}, store.LRU2)
		picker, err := NewClientPicker(addr, svcName, nil)
		if err != nil {
			cluster.close()
			tb.Fatalf("create picker for %s: %v", addr, err)
		}
		RegisterGroupToServer(group, srv)
		RegisterPeersToServer(picker, srv)
		cluster.servers = append(cluster.servers, srv)
	}

	for _, srv := range cluster.servers {
		if err := srv.Start(); err != nil {
			cluster.close()
			tb.Fatalf("start server %s: %v", srv.selfAddr, err)
		}
	}

	if err := cluster.waitForDiscovery(10 * time.Second); err != nil {
		cluster.close()
		tb.Fatalf("wait for discovery: %v", err)
	}

	for _, srv := range cluster.servers {
		conn, err := grpc.NewClient(srv.selfAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cluster.close()
			tb.Fatalf("dial entry client for %s: %v", srv.selfAddr, err)
		}
		cluster.clients = append(cluster.clients, conn)
	}

	cluster.entry = pb.NewCacheClient(cluster.clients[0])
	return cluster
}

func (c *benchmarkE2ECluster) waitForDiscovery(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, srv := range c.servers {
			if len(srv.clientPicker.grpcCli) != len(c.servers)-1 {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("service discovery timeout")
}

func (c *benchmarkE2ECluster) close() {
	for _, conn := range c.clients {
		if conn != nil {
			_ = conn.Close()
		}
	}
	for _, srv := range c.servers {
		if srv != nil {
			_ = srv.Close()
		}
	}
	if c.redisCli != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if c.prefix != "" {
			iter := c.redisCli.Scan(ctx, 0, c.prefix+":*", 0).Iterator()
			for iter.Next(ctx) {
				_ = c.redisCli.Del(ctx, iter.Val()).Err()
			}
		}
		cancel()
		_ = c.redisCli.Close()
	}
	if len(c.servers) > 0 {
		// Give etcd lease revocation and grpc shutdown a brief window to settle.
		time.Sleep(300 * time.Millisecond)
	}
}

func benchmarkFreeAddr(tb testing.TB) string {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve free port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func (c *benchmarkE2ECluster) key(i int) string {
	return fmt.Sprintf("%s:key:%d", c.prefix, i)
}

func (c *benchmarkE2ECluster) hotKey() string {
	return c.prefix + ":hot"
}

func benchmarkE2EValue(i int) []byte {
	return []byte("e2e-value-" + strconv.Itoa(i))
}

func BenchmarkRealDistributedCacheHotKeyParallel(b *testing.B) {
	cluster := newBenchmarkE2ECluster(b, benchmarkE2ENodeCount)
	defer cluster.close()

	ctx := context.Background()
	hotKey := cluster.hotKey()
	if _, err := cluster.entry.Set(ctx, &pb.Request{Key: hotKey, Value: []byte("hot-value")}); err != nil {
		b.Fatal(err)
	}
	if _, err := cluster.entry.Get(ctx, &pb.Request{Key: hotKey}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(loop *testing.PB) {
		for loop.Next() {
			resp, err := cluster.entry.Get(ctx, &pb.Request{Key: hotKey})
			if err != nil {
				b.Fatal(err)
			}
			if len(resp.Value) == 0 {
				b.Fatal("expected hot key hit")
			}
		}
	})
}

func BenchmarkRealDistributedCacheColdGetFromRedis(b *testing.B) {
	cluster := newBenchmarkE2ECluster(b, benchmarkE2ENodeCount)
	defer cluster.close()

	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		if _, err := cluster.entry.Set(ctx, &pb.Request{Key: cluster.key(i), Value: benchmarkE2EValue(i)}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := cluster.key(i & 4095)
		for _, srv := range cluster.servers {
			srv.groups.cache.Delete(key)
		}
		resp, err := cluster.entry.Get(ctx, &pb.Request{Key: key})
		if err != nil {
			b.Fatal(err)
		}
		if len(resp.Value) == 0 {
			b.Fatal("expected redis-backed cold hit")
		}
	}
}

func BenchmarkRealDistributedCacheParallelMixed(b *testing.B) {
	cluster := newBenchmarkE2ECluster(b, benchmarkE2ENodeCount)
	defer cluster.close()

	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		if _, err := cluster.entry.Set(ctx, &pb.Request{Key: cluster.key(i), Value: benchmarkE2EValue(i)}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(loop *testing.PB) {
		i := 0
		for loop.Next() {
			key := cluster.key(i & 4095)
			if i&7 == 0 {
				_, err := cluster.entry.Set(ctx, &pb.Request{Key: key, Value: benchmarkE2EValue(i)})
				if err != nil {
					b.Fatal(err)
				}
			} else {
				resp, err := cluster.entry.Get(ctx, &pb.Request{Key: key})
				if err != nil {
					b.Fatal(err)
				}
				if len(resp.Value) == 0 {
					b.Fatal("expected distributed hit")
				}
			}
			i++
		}
	})
}

func BenchmarkRealDistributedCacheFanoutReads(b *testing.B) {
	cluster := newBenchmarkE2ECluster(b, benchmarkE2ENodeCount)
	defer cluster.close()

	ctx := context.Background()
	keys := make([]string, 0, len(cluster.servers)*128)
	for i := 0; i < len(cluster.servers)*128; i++ {
		key := cluster.key(i)
		keys = append(keys, key)
		if _, err := cluster.entry.Set(ctx, &pb.Request{Key: key, Value: benchmarkE2EValue(i)}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(loop *testing.PB) {
		i := 0
		for loop.Next() {
			resp, err := cluster.entry.Get(ctx, &pb.Request{Key: keys[i%len(keys)]})
			if err != nil {
				b.Fatal(err)
			}
			if len(resp.Value) == 0 {
				b.Fatal("expected fanout hit")
			}
			i++
		}
	})
}

func BenchmarkRealDistributedCacheParallelMixedPerClient(b *testing.B) {
	cluster := newBenchmarkE2ECluster(b, benchmarkE2ENodeCount)
	defer cluster.close()

	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		if _, err := cluster.entry.Set(ctx, &pb.Request{Key: cluster.key(i), Value: benchmarkE2EValue(i)}); err != nil {
			b.Fatal(err)
		}
	}

	clients := make([]pb.CacheClient, len(cluster.clients))
	for i, conn := range cluster.clients {
		clients[i] = pb.NewCacheClient(conn)
	}

	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(loop *testing.PB) {
		clientIndex := 0
		for loop.Next() {
			client := clients[clientIndex%len(clients)]
			i := int(next.Add(1) - 1)
			key := cluster.key(i & 4095)
			if i&7 == 0 {
				_, err := client.Set(ctx, &pb.Request{Key: key, Value: benchmarkE2EValue(i)})
				if err != nil {
					b.Fatal(err)
				}
			} else {
				resp, err := client.Get(ctx, &pb.Request{Key: key})
				if err != nil {
					b.Fatal(err)
				}
				if len(resp.Value) == 0 {
					b.Fatal("expected hit")
				}
			}
			clientIndex++
		}
	})
}

func BenchmarkRealDistributedCacheParallelMixedMultiEntry(b *testing.B) {
	cluster := newBenchmarkE2ECluster(b, benchmarkE2ENodeCount)
	defer cluster.close()

	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		if _, err := cluster.entry.Set(ctx, &pb.Request{Key: cluster.key(i), Value: benchmarkE2EValue(i)}); err != nil {
			b.Fatal(err)
		}
	}

	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(loop *testing.PB) {
		for loop.Next() {
			i := int(idx.Add(1) - 1)

			client := pb.NewCacheClient(cluster.clients[i%len(cluster.clients)])
			key := cluster.key(i & 4095)
			if i&7 == 0 {
				_, err := client.Set(ctx, &pb.Request{Key: key, Value: benchmarkE2EValue(i)})
				if err != nil {
					b.Fatal(err)
				}
			} else {
				resp, err := client.Get(ctx, &pb.Request{Key: key})
				if err != nil {
					b.Fatal(err)
				}
				if len(resp.Value) == 0 {
					b.Fatal("expected hit")
				}
			}
		}
	})
}

func logrusStandardSilence() {
	logOutput := io.Discard
	if os.Getenv("BENCH_DEBUG") != "" {
		logOutput = os.Stderr
	}
	// Server code uses logrus package-level logger directly.
	// Benchmarks silence it by default to avoid skewing timings.
	logrus.SetOutput(logOutput)
}
