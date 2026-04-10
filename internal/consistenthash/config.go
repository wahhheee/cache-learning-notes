package consistenthash

import "hash/crc32"

// Config 定义一致性哈希的可调参数。
//
// 该配置主要影响三个方面：
//   - 哈希环中每个真实节点的虚拟节点数量。
//   - 负载均衡时副本数量可调整的上下限。
//   - 负载偏离平均值时，是否触发重新平衡。
type Config struct {
	// DefaultReplicas 是每个真实节点默认创建的虚拟节点数。
	//
	// 虚拟节点越多，哈希环切分通常越均匀，但维护成本也会更高。
	DefaultReplicas int

	// MinReplicas 是负载均衡过程中允许的最小虚拟节点数。
	//
	// 该值用于避免节点副本数被调整到过低，导致几乎不参与分片。
	MinReplicas int

	// MaxReplicas 是负载均衡过程中允许的最大虚拟节点数。
	//
	// 该值用于避免节点副本数被调整到过高，导致单节点占据过多区间。
	MaxReplicas int

	// HashFunc 是一致性哈希使用的哈希函数。
	//
	// 该函数同时用于：
	//   - 计算虚拟节点在环上的位置；
	//   - 计算业务 key 在环上的位置。
	HashFunc func(data []byte) uint32

	// MaxLoadBalanceThreshold 是负载均衡的上阈值。
	//
	// 若某节点负载 / 平均负载 > 该阈值，则认为该节点偏忙，
	// 会尝试减少其虚拟节点数。
	MaxLoadBalanceThreshold float64

	// MinLoadBalanceThreshold 是负载均衡的下阈值。
	//
	// 若某节点负载 / 平均负载 < 该阈值，则认为该节点偏闲，
	// 会尝试增加其虚拟节点数。
	MinLoadBalanceThreshold float64
}

// DefaultConfig 是一致性哈希模块的默认配置。
//
// 默认参数说明：
//   - 每个真实节点默认使用 50 个虚拟节点。
//   - 副本数最小不低于 10，最大不高于 200。
//   - 默认哈希函数使用 CRC32 IEEE。
//   - 当节点负载高于平均值 10% 或低于平均值 10% 时，触发重平衡判断。
var DefaultConfig = &Config{
	DefaultReplicas:         50,
	MinReplicas:             10,
	MaxReplicas:             200,
	HashFunc:                crc32.ChecksumIEEE,
	MaxLoadBalanceThreshold: 1.1,
	MinLoadBalanceThreshold: 0.9,
}
