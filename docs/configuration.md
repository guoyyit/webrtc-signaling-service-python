# 配置项说明

所有限制与时间参数集中在 `signaling/config.py` 的 `SignalingConfig` 中，
可通过三种方式覆盖（优先级从高到低）：

1. 代码中传入 `load_config(overrides={...}`；
2. 环境变量，命名为 `SIGNALING_<字段名大写>`；
3. 数据类默认值。

## 配置项

| 字段 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `room_capacity` | `SIGNALING_ROOM_CAPACITY` | `8` | 单个房间最大成员数，超出返回 `room_full` |
| `token_ttl` | `SIGNALING_TOKEN_TTL` | `3600.0` | 凭证有效期（秒），过期凭证无法加入 |
| `negotiation_timeout` | `SIGNALING_NEGOTIATION_TIMEOUT` | `15.0` | 等待 answer 的超时（秒），超时后结束并重试 |
| `max_retries` | `SIGNALING_MAX_RETRIES` | `3` | 同一成员对的最大协商重试次数，超出置为 `failed` |
| `heartbeat_interval` | `SIGNALING_HEARTBEAT_INTERVAL` | `5.0` | 期望客户端发送 ping 的间隔（秒） |
| `heartbeat_timeout` | `SIGNALING_HEARTBEAT_TIMEOUT` | `15.0` | 未收到心跳多久判定失联（秒） |
| `send_queue_size` | `SIGNALING_SEND_QUEUE_SIZE` | `64` | 单会话出站队列上界，满则隔离慢成员 |
| `send_timeout` | `SIGNALING_SEND_TIMEOUT` | `2.0` | 发送阻塞判定阈值（秒，预留） |

## 凭证密钥

凭证使用 HMAC-SHA256 签名。生产环境请通过环境变量设置密钥：

```bash
export SIGNALING_SECRET="your-strong-random-secret"
```

未设置时使用仅用于开发的默认密钥。

## 示例

```bash
# 容量 16、协商超时 20 秒、最多重试 5 次
SIGNALING_ROOM_CAPACITY=16 \
SIGNALING_NEGOTIATION_TIMEOUT=20 \
SIGNALING_MAX_RETRIES=5 \
uvicorn main:app --host 0.0.0.0 --port 8000
```

```python
from signaling.config import load_config

config = load_config({
    "room_capacity": 16,
    "negotiation_timeout": 20.0,
    "max_retries": 5,
})
server = build_server(config)
```

非法值（如容量为 0、负数超时）会在启动时抛出 `ValueError`；
未知配置键会被拒绝，避免拼写错误静默失效。
