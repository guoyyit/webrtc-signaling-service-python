# 协商流程说明

本文档描述多房间实时信令服务中，房间成员的发现、对等连接协商、冲突仲裁、
超时重试以及中转降级的完整流程。

## 1. 成员发现

1. 客户端通过 `POST /rooms` 创建房间，通过 `POST /rooms/{room_id}/tokens`
   获取带签名的凭证（token 携带 room_id、member_id、role、过期时间）。
2. 客户端建立 WebSocket：`/ws/{room_id}?token=...`。服务端校验凭证：
   - 无效、过期或与房间不匹配的凭证返回 `error(unauthorized)` 并关闭连接；
   - 房间不存在返回 `error(room_not_found)`；
   - 房间已满返回 `error(room_full)`。
3. 加入成功后，服务端向该成员发送 `joined`（含当前房间成员列表），
   并向房间内**其他**成员广播 `member_joined`。
4. 成员离开（主动 `leave`、断线、被踢、心跳超时）时，服务端广播
   `member_left` 并回收该成员的全部中转状态。

成员关系满足**幂等**：同一客户端重复加入或断线后重新加入，服务端只保留
一个成员；旧连接被新连接替换（不产生重复成员），在线状态与连接注册表
始终一致。

## 2. 协商消息的有序转发

- 协商消息包括 `offer`、`answer`、`ice_candidate`、`renegotiate`，以及
  链路状态 `link_report`。
- 所有协商消息**必须**携带 `target_id`，服务端只转发给该目标成员，
  绝不广播给无关成员。
- 服务端在单事件循环中按接收顺序处理帧，并通过每个会话的 FIFO 出站队列
  保证消息**有序**到达目标。
- 每个协商帧携带客户端序号 `seq`。状态机基于序号做幂等：
  - 重复帧（seq 不大于已处理序号）被丢弃，不改变当前协商阶段；
  - 乱序帧不会破坏已有结果（例如 answer 先于 offer 到达会被安全丢弃）。
- 无法识别的消息类型返回 `error(unknown_message_type)`，但**不会**中断
  该连接或其他成员的转发；畸形帧同理。

## 3. 协商冲突（glare）的确定仲裁

当双方**同时**向对方发起 offer 时，会出现冲突。服务端采用确定规则：

> 成员 id 字典序较小的一方为“不礼貌方”（impolite），仲裁获胜；
> 字典序较大的一方为“礼貌方”（polite），必须放弃自己的 offer。

流程：

1. `a` 向 `b` 发送 offer（a < b），服务端记录待处理 offer 来自 a。
2. `b` 同时向 `a` 发送 offer，服务端检测到双方都有未决 offer：
   - 若发送方是不礼貌方（a）：保留其 offer，向礼貌方（b）发送
     `negotiation_abandoned`（reason=`glare`），明确通知 b 结束本次协商；
   - 若发送方是礼貌方（b）：丢弃其 offer，向 b 发送
     `negotiation_abandoned`，b 的 offer 不转发给 a。
3. 被放弃的一方收到明确的结束通知后，可以：
   - 接受对方的 offer 并回复 `answer`，或
   - 通过 `renegotiate` 重置状态后重新发起。

由于规则只取决于成员 id 的字典序，与消息到达先后无关，因此结果唯一确定、
可复现，不会出现双方互相等待的僵局。

## 4. 协商超时与重试

- 服务端为每对成员的 offer 维护超时（`negotiation_timeout`）。
- 超时后仍未收到 answer：
  - 重试次数加一，阶段重置为 `idle`，并向**双方**发送
    `negotiation_abandoned`（reason=`timeout`），双方结束并可重新发起；
  - 当重试次数超过 `max_retries` 时，阶段置为 `failed`，通知 reason=`failed`，
    提示双方可降级到中转。
- 超时判定由注入的时钟驱动（`tick(now)`），在测试中可完全复现。

## 5. 中转降级与恢复

- 当直连不可用时，成员发送 `link_report`（payload `{"mode": "relay"}`）。
- 服务端记录该成员对的链路模式，并向房间广播 `link_state`
  （含 peer_a、peer_b、mode），让其他成员看到降级。
- 直连恢复时发送 `link_report`（`{"mode": "direct"}`），服务端同样广播
  恢复状态。
- 重复的相同模式报告被忽略，不产生重复通知。
- 降级/恢复只影响该成员对：其他成员对已建立的数据通道与协商**不被中断**。
- 成员离开时，与其相关的链路状态被清除。

## 6. 角色与越权控制

- 角色：`host`（主持人）、`member`（普通成员）、`viewer`（观众）。
- `kick` 仅 host 可发送；viewer 禁止发送 offer/answer/ice_candidate/
  renegotiate/link_report/kick。
- 越权消息返回 `error(forbidden)` 并记录可追溯的审计日志
  （`forbidden_control` / `forbidden_send`）。
- 所有关键动作（加入、离开、踢出、仲裁、超时、链路变化、慢成员驱逐）
  都通过 `signaling` logger 输出结构化事件，便于追溯。

## 7. 慢成员与失联隔离

- 每个会话的出站队列有上界（`send_queue_size`）。
- 成员消费过慢导致队列满时，服务端将其标记为隔离（isolated）并驱逐，
  **不阻塞**房间内其他成员的转发。
- 客户端需周期性发送 `ping`；服务端在 `heartbeat_timeout` 内未收到心跳
  则判定失联并驱逐，广播 `member_left`。
- 心跳超时判定是确定的：`(now - last_heartbeat) > timeout`（严格大于，
  边界时刻仍存活）。

## 8. 消息类型速查

| 方向 | 类型 | 说明 |
| --- | --- | --- |
| C→S | `join` / `leave` | 加入（重发快照）/ 离开 |
| C→S | `offer` / `answer` / `ice_candidate` / `renegotiate` | 协商 |
| C→S | `link_report` | 直连/中转模式上报 |
| C→S | `ping` / `kick` | 心跳 / 主持人踢出 |
| S→C | `joined` / `member_joined` / `member_left` | 成员发现 |
| S→C | `offer` / `answer` / `ice_candidate` | 协商转发 |
| S→C | `negotiation_abandoned` | 协商结束（glare/timeout/failed） |
| S→C | `link_state` | 链路模式变化 |
| S→C | `pong` / `error` | 心跳响应 / 错误 |
