# cache-learning-notes

`cache-learning-notes` 是一个学习向的 Go 分布式缓存项目。

本项目主要用于学习缓存系统、分布式节点发现、一致性哈希、gRPC 通信、Redis 落库以及缓存淘汰策略等相关内容，不建议直接用于生产环境。

## 项目来源

本项目基于以下开源项目进行学习、整理和改进：

- [youngyangyang04/KamaCache](https://github.com/youngyangyang04/KamaCache)
- [ChaoJiCaiNiao3/lcache_pro](https://github.com/ChaoJiCaiNiao3/lcache_pro)

其中 `lcache_pro` 是基于 `KamaCache` 的学习改进项目，本项目又在这两个项目的基础上继续学习和实现。

本项目尊重原项目的开源协议，并在 `LICENSE` 与 `NOTICE.md` 中保留相关许可和来源说明。

## 功能概览

- 本地缓存存储接口
- LRU / LRU2 缓存淘汰实现
- 一致性哈希与虚拟节点
- 基于 etcd 的服务注册与发现
- 基于 gRPC 的节点间通信
- 基于 Redis 的持久化存储
- singleflight 防止缓存击穿场景下的重复加载
- 命令行交互式测试入口

## 项目结构

```text
.
├── cmd/cache/                 # 程序入口
├── internal/consistenthash/    # 一致性哈希实现
├── internal/database/          # Redis 访问封装
├── internal/registry/          # etcd 服务注册
├── internal/server/            # 缓存服务、节点选择、gRPC 服务逻辑
├── internal/singleflight/      # singleflight 实现
├── pkg/pb/                     # protobuf 与 gRPC 生成代码
├── pkg/store/                  # 缓存存储接口及 LRU/LRU2 实现
├── Makefile
├── README.md
└── LICENSE
```

## 环境要求

- Go 1.26.1 或更高版本
- Redis
- etcd

默认配置：

- Redis: `localhost:6379`
- etcd: `localhost:2379`

相关默认配置可在以下文件中查看：

- `internal/database/config.go`
- `internal/registry/registry.go`
- `internal/consistenthash/config.go`

## 安装依赖

```bash
go mod tidy
```

## 构建

```bash
make build
```

或直接执行：

```bash
go build -trimpath -o bin/cache ./cmd/cache
```

## 运行

运行前请先确保 Redis 和 etcd 已启动。

```bash
make run
```

或直接执行：

```bash
go run ./cmd/cache
```

程序启动后会提示输入当前节点端口，例如：

```text
请输入端口:
8001
```

可以启动多个终端，分别输入不同端口，用于模拟多个缓存节点。

## 交互命令

启动后可输入以下命令进行简单测试：

| 命令 | 说明 |
| --- | --- |
| `get` | 根据 key 读取缓存或 Redis 数据 |
| `set` | 写入 key-value，并同步到 Redis |
| `delete` | 删除 key 对应的数据 |
| `set_hot` | 仅写入本地缓存，用于模拟热点数据 |
| `exit` | 关闭当前节点 |

## 测试

```bash
make test
```

或直接执行：

```bash
go test -v -race ./...
```

## 说明

本项目是学习过程中的实现，代码结构、异常处理、配置管理、日志和测试覆盖都还有继续完善的空间。

如果你也在学习分布式缓存，可以重点关注以下部分：

- 缓存接口如何抽象
- LRU / LRU2 如何管理缓存淘汰
- 一致性哈希如何选择目标节点
- 节点上下线如何通过 etcd 感知
- gRPC 如何在节点之间转发缓存请求
- 缓存未命中时如何通过 Redis 回源
- singleflight 如何减少重复回源请求

## 许可证

由于本项目基于 `KamaCache` 及相关学习改进项目继续实现，其中 `KamaCache` 使用 GPL-3.0 许可证。为保持许可证兼容，本项目采用 `GPL-3.0-or-later` 许可证发布。

详情见 [LICENSE](./LICENSE)。

上游项目来源及版权说明见 [NOTICE.md](./NOTICE.md)。
