# Message Delivery Lab — 死信重放工作台

在消息交付工作台中筛选死信、冻结为重放批次、在开始前确认预览，然后按派生幂等键重放。
原始消息与失败历史全程只读；批次是唯一可变聚合体。

Run `npm install`, then `npm run dev`（Vite http://localhost:4173 → API http://127.0.0.1:4174）。
`npm test` 运行 14 个 vitest/supertest 用例；`npm run build` 做类型检查与前端构建。

## 一致性模型

- **筛选期新增死信**：筛选预览是实时计数；点击“冻结”才快照。冻结后到达的匹配死信不计入批次，仅在批次详情中显示为 `driftAdded`，需要时另建批次。
- **冻结的配置摘要**：目标主题与头部补丁在冻结时随集合一起固化。批次开始前可凭 `revision` 修改（PATCH）；开始后配置锁定。
- **派生幂等键**：`replay:<batchId>:<originalMessageId>`，冻结时派生、重试不变。broker 对已接受的键短路返回 `duplicate`，不再次触达目标，因此重试绝不会让成功消息重复发送。
- **部分失败**：`POST /api/batches/:id/retry` 仅把仍为 `failed` 的项重新置队，成功项不再发送。
- **取消**：取消后泵循环不再启动任何新发送；在飞项自然落地并保留其结果，未开始项标记 `cancelled`，已成功项保留。
- **revision 乐观锁**：开始/重试/取消/改配置都需携带当前 revision；过期请求得到 `409 revision_conflict` 并返回 `current` 真相，两个页面无法同时继续或回滚。重复 start（双击/双页）在状态上幂等，不会产生第二个发送泵。
- **断线恢复**：SSE `/api/batches/:id/events?after=<seq>` 先重放错过的事件，再续推实时事件；事件按批次单调 seq 编号。前端按**原始消息 id** 归并 item 事件，并有 2.5s 轮询兜底。

## HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/dead-letters?topic=&reason=` | 不可变死信列表（含 payload/headers/history） |
| `POST /api/dead-letters/preview` | 实时（未冻结）匹配计数 |
| `POST /api/batches` | 冻结筛选结果 + 转换配置，返回确定预览数据 |
| `GET /api/batches/:id/preview` | 开始前确认预览：冻结集合、目标主题、生效头部、原始载荷/历史 |
| `PATCH /api/batches/:id` | 开始前修改目标主题/头部（需 revision） |
| `POST /api/batches/:id/start` `/retry` `/cancel` | 发送控制（均需 revision） |
| `GET /api/batches/:id/events?after=<seq>` | SSE 事件流（item/batch，断线按 seq 续传） |
| `POST /api/dev/dead-letters` | 演示/测试用：追加一条新死信（只追加） |

## 故障模拟（无需改 broker）

- 头部 `x-sim: flaky`：每个键首次发送失败、重试成功。
- 头部 `x-sim: fail-always`：始终失败。
- 目标主题 `topic.rejected`（或以 `.rejected` 结尾）：目标主题被 broker 拒绝。
- “模拟筛选期间新增死信”按钮在冻结前后均可点击，用于观察实时匹配与冻结集合的差异。

## 代码结构

- `src/shared/protocol.ts` — 前后端共享类型。
- `src/server/store.ts` — 只追加的死信日志、批次聚合、事件编号与计数。
- `src/server/broker.ts` — 幂等生产者（去重内存）+ 可注入失败策略。
- `src/server/service.ts` — 冻结快照、revision CAS、串行发送泵、取消/重试、事件订阅/重放。
- `src/server/index.ts` — Express 路由与 SSE。
- `src/client/` — 三栏工作台（筛选与转换 / 冻结预览与进度 / 原始消息检视）。
- `test/` — broker 幂等单测 + 七个竞态/故障场景的端到端用例。
