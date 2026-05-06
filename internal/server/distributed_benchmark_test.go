package server

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"testing"
	"time"

	pb "cache/pkg/pb"
	"cache/pkg/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const benchmarkClusterNodeCount = 4
const benchmarkBufconnSize = 1024 * 1024

type benchmarkDistributedNode struct {
	pb.UnimplementedCacheServer
	addr    string
	cluster *benchmarkDistributedCluster
	cache   store.Store
	server  *grpc.Server
	lis     *bufconn.Listener
	conn    *grpc.ClientConn
	client  pb.CacheClient
}

type benchmarkDistributedCluster struct {
	nodes []*benchmarkDistributedNode
	entry pb.CacheClient
}

func newBenchmarkDistributedCluster(tb testing.TB, nodeCount int) *benchmarkDistributedCluster {
	tb.Helper()

	cluster := &benchmarkDistributedCluster{
		nodes: make([]*benchmarkDistributedNode, 0, nodeCount),
	}

	for i := 0; i < nodeCount; i++ {
		node := &benchmarkDistributedNode{
			addr: fmt.Sprintf("node-%d", i),
			cache: store.NewStore(store.LRU2, store.Options{
				BucketCount:     64,
				CapPerBucket:    1024,
				Level2Cap:       1024,
				CleanupInterval: time.Hour,
			}),
			server: grpc.NewServer(),
			lis:    bufconn.Listen(benchmarkBufconnSize),
		}
		node.cluster = cluster
		pb.RegisterCacheServer(node.server, node)
		cluster.nodes = append(cluster.nodes, node)
	}

	for _, node := range cluster.nodes {
		go func(node *benchmarkDistributedNode) {
			_ = node.server.Serve(node.lis)
		}(node)

		conn, err := grpc.NewClient(
			"passthrough:///"+node.addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return node.lis.DialContext(ctx)
			}),
		)
		if err != nil {
			tb.Fatalf("dial %s: %v", node.addr, err)
		}
		node.conn = conn
		node.client = pb.NewCacheClient(conn)
	}

	cluster.entry = cluster.nodes[0].client
	return cluster
}

func (c *benchmarkDistributedCluster) close() {
	for _, node := range c.nodes {
		if node.conn != nil {
			_ = node.conn.Close()
		}
		node.server.Stop()
		node.cache.Close()
		_ = node.lis.Close()
	}
}

func (c *benchmarkDistributedCluster) owner(key string) *benchmarkDistributedNode {
	idx := int(crc32.ChecksumIEEE([]byte(key)) % uint32(len(c.nodes)))
	return c.nodes[idx]
}

func (n *benchmarkDistributedNode) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	owner := n.cluster.owner(req.Key)
	if owner != n {
		return owner.client.Get(ctx, req)
	}

	value, ok := n.cache.Get(req.Key)
	if !ok {
		return &pb.ResponseForGet{}, nil
	}

	return &pb.ResponseForGet{Value: value.(store.ByteView)}, nil
}

func (n *benchmarkDistributedNode) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	owner := n.cluster.owner(req.Key)
	if owner != n {
		return owner.client.Set(ctx, req)
	}

	value := store.ByteView(req.Value)
	if err := n.cache.Set(req.Key, value); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: value}, nil
}

func (n *benchmarkDistributedNode) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	owner := n.cluster.owner(req.Key)
	if owner != n {
		return owner.client.Delete(ctx, req)
	}

	return &pb.ResponseForDelete{Value: n.cache.Delete(req.Key)}, nil
}

func benchmarkDistributedKey(i int) string {
	return "dist-key-" + strconv.Itoa(i)
}

func benchmarkDistributedValue(i int) []byte {
	return []byte("dist-value-" + strconv.Itoa(i))
}

func BenchmarkDistributedCacheSet(b *testing.B) {
	cluster := newBenchmarkDistributedCluster(b, benchmarkClusterNodeCount)
	defer cluster.close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := cluster.entry.Set(ctx, &pb.Request{Key: benchmarkDistributedKey(i), Value: benchmarkDistributedValue(i)})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDistributedCacheGetHit(b *testing.B) {
	cluster := newBenchmarkDistributedCluster(b, benchmarkClusterNodeCount)
	defer cluster.close()

	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		_, err := cluster.entry.Set(ctx, &pb.Request{Key: benchmarkDistributedKey(i), Value: benchmarkDistributedValue(i)})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := cluster.entry.Get(ctx, &pb.Request{Key: benchmarkDistributedKey(i & 4095)})
		if err != nil {
			b.Fatal(err)
		}
		if len(resp.Value) == 0 {
			b.Fatal("expected hit")
		}
	}
}

func BenchmarkDistributedCacheGetMiss(b *testing.B) {
	cluster := newBenchmarkDistributedCluster(b, benchmarkClusterNodeCount)
	defer cluster.close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := cluster.entry.Get(ctx, &pb.Request{Key: benchmarkDistributedKey(i)})
		if err != nil {
			b.Fatal(err)
		}
		if len(resp.Value) != 0 {
			b.Fatal("expected miss")
		}
	}
}

func BenchmarkDistributedCacheParallelMixed(b *testing.B) {
	cluster := newBenchmarkDistributedCluster(b, benchmarkClusterNodeCount)
	defer cluster.close()

	ctx := context.Background()
	for i := 0; i < 4096; i++ {
		_, err := cluster.entry.Set(ctx, &pb.Request{Key: benchmarkDistributedKey(i), Value: benchmarkDistributedValue(i)})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(loop *testing.PB) {
		i := 0
		for loop.Next() {
			key := benchmarkDistributedKey(i & 4095)
			if i&7 == 0 {
				_, err := cluster.entry.Set(ctx, &pb.Request{Key: key, Value: benchmarkDistributedValue(i)})
				if err != nil {
					b.Fatal(err)
				}
			} else {
				_, err := cluster.entry.Get(ctx, &pb.Request{Key: key})
				if err != nil {
					b.Fatal(err)
				}
			}
			i++
		}
	})
}

func BenchmarkDistributedCacheHotKeyParallel(b *testing.B) {
	cluster := newBenchmarkDistributedCluster(b, benchmarkClusterNodeCount)
	defer cluster.close()

	ctx := context.Background()
	const hotKey = "dist-hot-key"
	_, err := cluster.entry.Set(ctx, &pb.Request{Key: hotKey, Value: []byte("dist-hot-value")})
	if err != nil {
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
