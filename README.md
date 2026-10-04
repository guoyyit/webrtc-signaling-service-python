# 多房间实时信令与连接协调服务

基于 FastAPI 的 WebRTC 信令服务：多个客户端通过长连接（WebSocket）加入同一
房间后互相发现、有序转发协商消息；直连不可用时降级到服务端中转；并提供
鉴权、角色、冲突仲裁、超时重试、慢成员隔离等能力。

## 特性

- **房间与成员**：创建/加入/退出，重复加入与断线重连幂等，不产生重复成员，
  成员关系与在线状态始终一致。
- **有序协商转发**：`offer`/`answer`/`ice_candidate` 只到达指定对端，不广播；
  序号幂等，乱序与重复不破坏协商结果；未知消息类型不影响其他成员。
- **冲突仲裁**：双方同时发起 offer 时，按成员 id 字典序确定唯一获胜方，
  被放弃方收到明确的 `negotiation_abandoned` 并可重新发起，无僵局。
- **鉴权与角色**：HMAC 签名凭证（含过期时间），支持 host/member/viewer 角色；
  越权控制消息被拒绝并记录可追溯审计日志。
- **中转降级与恢复**：`link_report` 触发 `relay`/`direct` 模式切换并通知
  全房间；其他成员对已建立的数据通道不中断。
- **慢成员隔离**：有界出站队列，消费过慢或心跳超时的成员被确定性交由
  `tick` 驱逐，不阻塞房间。
- **可配置**：房间容量、凭证有效期、协商超时、重试次数、心跳间隔、队列上界
  均可通过环境变量或代码配置。

## 快速开始

```bash
# 安装依赖（已使用 uv）
uv sync

# 启动
uvicorn main:app --host 127.0.0.1 --port 8000
```

### HTTP API

```bash
# 1. 创建房间
curl -X POST http://127.0.0.1:8000/rooms \
  -H 'Content-Type: application/json' \
  -d '{"room_id": "room-1"}'

# 2. 获取凭证（role: host / member / viewer）
curl -X POST http://127.0.0.1:8000/rooms/room-1/tokens \
  -H 'Content-Type: application/json' \
  -d '{"member_id": "alice", "role": "host"}'

# 3. 查询房间成员
curl http://127.0.0.1:8000/rooms/room-1
```

### WebSocket

```
ws://127.0.0.1:8000/ws/room-1?token=<TOKEN>
```

客户端发送 JSON 帧，例如向 `bob` 发起协商：

```json
{"type": "offer", "target_id": "bob", "seq": 1, "payload": {"sdp": "..."}}
```

完整消息类型与协商流程见 [docs/negotiation.md](docs/negotiation.md)。

## 配置

见 [docs/configuration.md](docs/configuration.md)。关键环境变量：

```bash
export SIGNALING_SECRET="your-strong-secret"
export SIGNALING_ROOM_CAPACITY=8
export SIGNALING_NEGOTIATION_TIMEOUT=15
export SIGNALING_MAX_RETRIES=3
export SIGNALING_HEARTBEAT_TIMEOUT=15
```

## 测试

```bash
pytest -q
```

测试覆盖：重复加入、协商冲突（glare）、中转降级、慢成员与心跳超时、
断线重连、鉴权与角色、未知消息类型、配置加载、HTTP/WebSocket 端到端。

## 项目结构

```
signaling/
  config.py       # 配置项与加载
  models.py       # 数据类型与消息枚举
  auth.py         # HMAC 凭证签发与校验
  room.py         # 房间与成员管理
  session.py      # 连接会话、有界队列、心跳
  negotiation.py  # 协商状态机与冲突仲裁
  relay.py        # 中转降级/恢复
  server.py       # 信令核心：路由、有序转发、审计
main.py           # FastAPI 入口：HTTP + WebSocket 传输
tests/            # 自动化测试
docs/             # 文档
```
