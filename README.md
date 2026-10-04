# WebRTC Signaling Service

多房间实时信令与连接协调服务。客户端通过 WebSocket 长连接加入房间，
服务负责成员发现、协商消息的有序定向转发、协商冲突（glare）裁决、
直连不可用时的服务端中转降级/恢复，以及慢成员与失联成员隔离。

## 运行

```bash
uv sync
uv run python main.py          # 监听 127.0.0.1:8000
uv run pytest tests/ -q        # 运行测试
```

客户端连接：`ws://<host>:8000/ws/<room>?token=<token>`。
token 为 HMAC-SHA256 签名的访问凭证（见「接入鉴权」）。

## 架构

```
signaling/
  config.py       配置项（容量/超时/重试/心跳）
  models.py       消息类型、角色、错误码
  auth.py         令牌签发与校验（HMAC-SHA256，含过期时间）
  negotiation.py  协商状态机：glare 裁决、去重、超时重试（纯逻辑）
  relay.py        中转状态管理（按成员对）
  room.py         房间：成员关系、有序转发、慢成员隔离、心跳判定
  service.py      服务编排：鉴权、消息分发、审计日志、周期清扫
  server.py       FastAPI/WebSocket 传输层
```

核心逻辑（`negotiation`/`relay`/`room`/`service`）与传输层解耦，
测试直接用内存假连接驱动 `SignalingService`。

## 消息协议

客户端 → 服务端：

| type         | 字段                       | 说明                             |
|--------------|----------------------------|----------------------------------|
| `offer`      | `target`, `seq`, `payload` | 发起协商（SDP offer）            |
| `answer`     | `target`, `seq`, `payload` | 应答协商                         |
| `ice`        | `target`, `seq`, `payload` | ICE candidate，纯转发            |
| `renegotiate`| `target`, `seq`, `payload` | 重新协商提示，纯转发             |
| `relay_start`| `target`                   | 声明与该对端直连不可用，请求中转 |
| `relay_stop` | `target`                   | 声明直连恢复，停止中转           |
| `relay_data` | `target`, `seq`, `payload` | 经服务端中转的数据（需中转已激活）|
| `ping`       | -                          | 心跳（任何消息都刷新存活时间）   |
| `leave`      | -                          | 主动离开                         |

服务端 → 客户端：`join_ack`、`peer_joined`、`peer_left`、`offer`/`answer`/
`ice`/`renegotiate`（带 `sender`、`seq` 与单调递增的 `server_seq`）、
`glare_resolved`、`relay_state`、`relay_data`、`ping`/`pong`、`error`。

### 成员一致性

成员集合即在线集合：离开、断线、心跳超时、慢消费被隔离都会移除成员并
广播 `peer_left`。同一 `client_id` 重复加入时，旧连接被关闭
（code 4000），成员记录原位更新，**不会产生重复成员**，其他成员也不会
收到重复的 `peer_joined`。

### 协商流程与冲突裁决

1. A 向 B 发送 `offer`（携带客户端侧单调递增的 `seq`）。
2. 服务端按 `(pair, sender)` 记录序号：重复或乱序的 `offer`/`answer`
   被静默丢弃，不会破坏协商状态。
3. **Glare（双方同时发起）**：按确定规则裁决——字典序较大的
   `client_id` 为 *polite* 一方，其 offer 获胜：
   - impolite 方收到 `glare_resolved{action:"rollback", winner}`，
     放弃自己的 offer，随后应答对方 offer；
   - polite 方收到 `glare_resolved{action:"ignore_peer_offer"}`，
     忽略对方在冲突发生前已送达的 offer。
   双方用同一规则计算，结果唯一，不会互相等待。
4. 被放弃的一侧在协商结束后可以用更大的 `seq` 重新发起。
5. `offer` 超时未收到 `answer`：服务端按 `negotiation_timeout_s`
   重新转发，最多 `max_negotiation_retries` 次；仍失败则清除状态并
   向发起方发送 `error`，双方可重新发起。

### 中转降级与恢复

- `relay_start`/`relay_stop` 是**控制类消息**，仅 `host` 与
  `participant` 角色可发送；`viewer` 越权发送会被拒绝并写入审计日志。
- 状态变化以 `relay_state{a, b, active}` 广播给**房间内所有成员**，
  降级与恢复全程可见。
- 中转切换只翻转状态位，不触碰成员与队列，已建立的数据通道不中断。
- `relay_data` 仅在该对端对中转激活时转发，否则返回 `forbidden` 错误。

### 慢成员与失联成员隔离

- 每个成员有容量为 `max_pending_messages` 的发送队列；消费过慢导致
  队列溢出时，该成员被立即隔离（关闭连接 code 4001、移除、广播
  `peer_left`），不影响其他成员的转发。
- 心跳判定是纯函数：`now - last_seen > heartbeat_timeout_s` 即超时，
  确定且可复现；任何消息（含 `ping`）都刷新 `last_seen`。

## 接入鉴权

令牌格式：`base64url(payload).base64url(hmac_sha256)`，payload 含
`room`、`client_id`、`role`、`exp`。无效签名、过期、跨房间的令牌
都会被拒绝并记入 `service.audit_log`（含时间、房间、动作、详情），
越权控制消息同样可追溯。

角色：`host` / `participant` / `viewer`；控制类消息
（`relay_start`/`relay_stop`）仅前两者可用。

签发示例：

```python
from signaling import SignalingConfig, SignalingService
from signaling.models import Role
svc = SignalingService(SignalingConfig(token_secret="s3cret"))
token = svc.issuer.issue("room-1", "alice", Role.HOST, now=time.time())
```

## 配置项（`SignalingConfig`）

| 字段                       | 默认值  | 说明                           |
|----------------------------|---------|--------------------------------|
| `max_room_capacity`        | 16      | 房间最大成员数                 |
| `negotiation_timeout_s`    | 10.0    | offer 等待 answer 的超时       |
| `max_negotiation_retries`  | 3       | offer 超时后的最大重转次数     |
| `heartbeat_interval_s`     | 5.0     | 服务端 ping / 清扫间隔         |
| `heartbeat_timeout_s`      | 15.0    | 成员失联判定超时               |
| `max_pending_messages`     | 256     | 单成员发送队列容量（慢成员阈值）|
| `token_ttl_s`              | 300.0   | 访问令牌有效期                 |
| `token_secret`             | dev-... | 令牌 HMAC 密钥                 |

## 测试覆盖

`tests/` 共 26 个用例，覆盖：重复加入/断线重连无重复成员、房间容量、
成员=在线集合一致性、定向转发不广播、乱序/重复消息、glare 双方同时
发起与败方重发、协商超时重试与失败、未知消息类型不中断他人、令牌
无效/过期/跨房间、越权控制消息与审计、中转降级/恢复的房间级可见性、
数据通道不中断、慢成员隔离、心跳超时边界确定性、WebSocket 端到端
（含重连）。
