# Ravel

独立的节点、机器监控与 TCP/UDP 转发运行时。节点使用 Xray 与 Ravel/1 核心，转发使用 GOST；保留这些组件的协议和许可，不依赖另一个节点管理程序。

## 软件安装

### 一键安装

```
curl -fLo ravel-install.sh https://raw.githubusercontent.com/YeJianbo/ravel/main/script/ravel-install.sh
bash ravel-install.sh --panel https://panel.example.com --machine-id 1
```

## 构建

使用 Go 1.26.8 或更高版本。当前 `go.mod` 将 `github.com/YeJianbo/ravel-go`
替换为 `../ravel-go`，构建前需要把匹配的共享核心源码放在节点仓库的同级目录。
仅克隆本仓库不包含该依赖；独立 CI 和容器构建也需要显式提供共享核心源码。

``` bash
GOEXPERIMENT=jsonv2 go build -o build_assets/ravel -trimpath -ldflags "-X 'github.com/YeJianbo/ravel/cmd.version=$version' -s -w -buildid="
```

容器构建使用 Buildx 命名上下文提供同一份共享核心，不依赖镜像构建时拉取远端最新源码：

```bash
docker buildx build --build-context ravel_core=../ravel-go --load -t ravel:local .
```

CI 的 `docker/build-push-action` 需通过 `build-contexts` 显式传入已检出的匹配核心。

## 配置加密

支持将节点配置文件加密后落盘，运行时通过环境变量解密：

```bash
ravel config keygen
export RAVEL_CONFIG_KEY='上一步输出的密钥'
ravel config encrypt --in /etc/ravel/config.json --out /etc/ravel/config.enc.json
ravel server -c /etc/ravel/config.enc.json
```

机器模式使用 `ravel run`，默认配置位于 `/etc/ravel`，程序位于 `/usr/local/ravel/ravel`，服务名为 `ravel`。密钥可以通过 `RAVEL_CONFIG_KEY_FILE` 指定文件，避免放进服务参数。

已有安装使用 `bash ravel-install.sh --migrate` 显式迁移。迁移先备份旧配置及服务文件，不删除旧文件；普通安装不会自动接管旧服务。升级到新客户端前需要先发布面板的 `ravelConfig` 接口和 `node_type=ravel` 兼容入口。仅有本地源码不代表独立 GitHub 仓库或发布包已经上线。

安装后使用 `ravel start/stop/restart/status/log` 管理服务，`ravel update --version vX.Y.Z` 通过本机保存的可信安装器更新。机器 Token 只在首次安装时隐藏输入，自动部署使用 `--token-stdin`，不要放进命令参数。

Ravel 同步会将 `_BunCloudManagedNodes` 托管身份与 `Nodes` 一起写入加密配置，
通过同一次原子替换提交；普通节点配置加载器忽略该附加字段。
旧 `managed-nodes.json` 仍作为兼容副本维护，但新版不再依赖它与配置同时写入。
若兼容副本更新出现告警，回退到不识别附加字段的旧客户端前，应先确认副本与当前配置一致。
只更新托管身份不会触发节点重载，手动节点及其他本地配置仍沿用原合并逻辑保留。
新版身份包含面板地址和 NodeID，避免不同面板的同号节点互相覆盖。
旧数字身份只在归属唯一时自动迁移；遇到跨面板同号冲突时中止同步并保留原文件，需先确认归属。
兼容副本仍使用旧数字格式，旧客户端不能区分跨面板同号节点，混用多面板时不要直接回退。

## 机器端更新信任边界

Ravel 运行时只信任程序内固定的 `YeJianbo/ravel` 更新仓库，发布包名为 `ravel-linux-<架构>.zip`。
面板不能通过 `auto_update.repo` 改变代码来源；未知仓库在网络下载和文件替换前被拒绝。
Ravel 的面板请求只允许升级或保持同版本，不允许远程降级，返回的 Release 必须匹配明确请求的版本。
已下载的正式版本继续使用现有摘要校验和升级失败回滚；本机管理员主动恢复上一版本的机制保留。
该边界不防护可信 GitHub 仓库或发布流程本身失陷，也不等于机器已安装新版。

探针拨测不直接运行面板提供的完整 sing-box 配置，而是按现有协议字段重建本机配置。
日志、DNS、监听和路由由本机固定生成，禁止出站引用本机文件或添加未知功能。
拨测目标固定为 Cloudflare 的连通性检查地址，单次请求时长最多 30 秒，编码配置最多 87384 字节。
节点地址仍是面板管理的数据，该限制不隔离节点网络访问，也不替代进程权限隔离。

## 网络质量采集

ICMP 采集需要可执行且有权限的 `ping` 工具。命令启动失败、超时或返回不完整统计时，
本轮不上报，错误记录在 Ravel 日志；不会将采集故障伪装成 100% 丢包。
完整统计中的真实丢包仍正常上报，丢包率由实际发送和接收数量计算。
面板要求所有启用目标一起上报，因此一个目标采集失败时整轮跳过，下一周期继续采集。

## 转发监听检查

外部 GOST 的监听检查仅保留配置端口对应的监听 inode，分批读取进程文件描述符并核对归属，
所需端口确认后提前结束；不会将该进程的所有活动连接 inode 装入内存。
连接表直接从扫描缓冲区提取所需字段，非监听连接在读到状态字段时跳过，避免为每条连接创建整行字符串和字段列表。
套接字扫描在控制状态锁外执行，不阻塞停止、重启或规则更新；扫描期间若进程或规则变更，
该次上报仅返回最新转发状态，不使用过时的端口检查结果，下一次上报重新检查。

## 许可与来源

代码来源及许可见 `NOTICE` 和 `LICENSE`。产品独立命名不改变第三方组件的版权或许可，也不隐藏实际连接用途。
