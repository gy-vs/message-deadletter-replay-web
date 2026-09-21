# Message Delivery Lab — 死信重放工作台

筛选死信、生成**冻结**的重放批次（可修改目标主题与头部），经确定预览后串行发送；
原始消息与失败历史始终不可变。

运行：`npm install`（如遇 registry 网络问题可加 `--registry=https://registry.npmmirror.com`），
然后 `npm run dev`（服务端 4174 / 前端 4173）。测试：`npm test`。

## 语义

- **冻结集合**：`POST /api/batches` 在服务端按当前筛选快配匹配集合与转换摘要
  （目标主题、头部 set/remove）。筛选期间新增的死信不会进入已创建的批次；
  预览数量始终等于冻结数量（`items.length`），另记录冻结时死信池总量 `filter.candidateTotal`。
- **不可变**：死信（含失败历史）只读且对象被冻结；重放状态保存在批次条目中，不回写原消息。
- **派生幂等键**：每个条目由 `批次 id + 原消息 id + 目标主题 + 转换后头部` 派生
  `rk_<sha256:32>`；Broker 在任何校验前按幂等键去重，重复调用不产生第二次出站投递。
- **revision 乐观锁**：每个事件推进一次批次 revision。`start / cancel / retry`
  必须携带当前 revision，过期返回 `409 revision_conflict` 并附 `current` 快照；
  重复 `start` 即使 revision 最新也返回 `409 invalid_state`。
- **取消即回滚**：`cancel` 后不启动新发送（串行执行，至多一条在途），未发送项标记
  `cancelled`，已成功项保留，批次终结为 `partial/cancelled`。
- **仅重试失败项**：`retry` 只重放瞬时失败（`failed`）的条目；成功项不重发，
  目标永久拒绝（`rejected`）需带新转换另建批次。
- **配置防漂移**：开始时若提交的转换与冻结摘要不一致，返回 `422 config_mismatch`，
  冻结配置不变。
- **事件与断线恢复**：`GET /api/batches/:id/events?after=<seq>` 为 SSE，连接时先下发
  完整 `snapshot`，再重放缺失增量，后续推实时事件。前端按**原消息 id** 归并 item
  事件、按 `seq` 去重，断线自动重连后以 snapshot 校准。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/dead-letters?q&topic&reason` | 筛选（只读） |
| POST | `/api/dead-letters` | 模拟新增死信（用于验证冻结边界） |
| POST | `/api/batches` | 冻结筛选 + 转换，返回预览批次 |
| POST | `/api/batches/:id/start` | body `{revision, transform?}`，转换须与冻结一致 |
| POST | `/api/batches/:id/cancel` | body `{revision}` |
| POST | `/api/batches/:id/retry` | body `{revision}`，仅重试 failed |
| GET | `/api/batches/:id` | 当前快照 |
| GET | `/api/batches/:id/events?after=seq` | SSE：snapshot + 增量 |

测试用头部 `x-dlq-sim: fail-once | reject` 控制 broker 模拟瞬时失败与目标拒绝。
