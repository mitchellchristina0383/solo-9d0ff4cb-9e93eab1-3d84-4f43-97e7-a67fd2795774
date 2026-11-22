# 园区电视组播换台 API

FastAPI 实现的园区电视组播换台代理，配套一个本地模拟节目源（可延迟发出首包、可拒绝加入）。
机顶盒（STB）按频道号选台、退订，并读取代理转发的节目包。

## 架构

```
机顶盒 --HTTP--> 代理 proxy:8000 --HTTP(join/leave)--> 模拟节目源 source:9000
机顶盒 <--HTTP读包-- 代理 <--UDP 节目包(模拟组播转发)-- 节目源
```

- `source/`：模拟节目源。控制面 HTTP（join/leave/config/state），数据面 UDP 周期性发节目包。
  每个频道模拟一个组播组 `239.0.0.<频道号>`，可配置首包延迟 `delay`、拒绝加入 `refuse`、发包间隔 `interval`。
- `proxy/`：换台代理（演示入口）。维护机顶盒状态与每频道一路上游订阅；
  节目包按频道只缓存一份，各机顶盒凭游标读取。
- `tests/selftest.py`：端到端自测。

## 启动（Docker Compose）

```bash
docker compose up --build
```

- 机顶盒 API 演示入口： http://localhost:8000/docs （Swagger UI，根路径 `/` 会重定向到这里）
- 节目源控制面： http://localhost:9000/docs

本机无 Docker 时也可裸跑（Python 3.11+，需两个进程）：

```bash
pip install -r source/requirements.txt -r proxy/requirements.txt
cd source && PROXY_HOST=127.0.0.1 PROXY_UDP_PORT=5000 uvicorn app:app --port 9000 &
cd proxy && SOURCE_URL=http://127.0.0.1:9000 uvicorn app:app --port 8000 &
```

## 在 /docs 演示：两台同看后其中一台换台

1. `POST /stbs/stb-1/select` 与 `POST /stbs/stb-2/select`，请求体均为 `{"channel": 1}`
   → 均返回 `202 pending_first_packet`（等待首包）。
2. `GET /stbs/stb-1/status`、`GET /stbs/stb-2/status` → 首包到达后 `current_channel` 都变为 `1`。
3. `GET /state` → 频道 1 的 `watchers` 为 `[stb-1, stb-2]`，源侧 `joins == 1`（只一路订阅）；
   `buffered_packets` 约等于单台 `queued_packets` 而非两倍（节目包只缓存一份）。
4. `GET /stbs/stb-2/packets` 读走 stb-2 的包 → `GET /stbs/stb-1/status` 的 `queued_packets` 不受影响。
5. `POST /stbs/stb-1/select` 请求体 `{"channel": 2}` → `202 pending_first_packet`，首包到达后切到频道 2。
6. `GET /stbs/stb-1/packets` → 换台前频道 1 的积压旧包仍在，且按到包顺序排在频道 2 的新包之前。
7. `GET /stbs/stb-2/packets` → stb-2 持续收到频道 1 的包，不受 stb-1 换台影响。

演示时实测输出（裸进程方式，与 compose 同一套代码）：

```
### 3. 两台都在看频道1, 上游只一路订阅, 节目包只缓存一份
{ "stb-1": 5, "stb-2": 5, "ch1_buffered": 5, "ch1_watchers": ["stb-1", "stb-2"], "source_joins": 1 }
### 4. stb-2 读走全部 (不影响 stb-1)
{ "stb-2_returned": 5, "stb-1_queued": 6 }
### 6. stb-1 换台后读包: 旧台1的积压包仍在, 按到包顺序排在新台2之前
{ "returned": 9, "channels_seq": [1,1,1,1,1,1,2,2,2] }
### 7. stb-2 仍在看频道1并持续收包
{ "stb-2_queued": 3, "ch1": {"watchers": ["stb-2"], "buffered": 3}, "ch2": {"watchers": ["stb-1"], "buffered": 0} }
```

其他可演示项：在源侧 `POST /config/2` 设置 `{"delay": 3}` 后选台频道 2，可观察"等待首包"期间旧台照常收包；
设置 `{"refuse": true}` 可模拟加入失败（机顶盒停留在旧台，`last_error` 可见原因）；
`POST /stbs/stb-1/unsubscribe` 退订后，`GET /stbs/stb-1/packets` 仍能读出退订前的未读旧包。

## 核心行为（实现要点）

### 控制面（换台）

- **同一频道只向上游订阅一路**：收看者与等待首包者共享引用计数；`join` 幂等，进行中的加入不会重复发起。
- **无人收看且无人等候首包时退订**：引用计数归零即向上游 `leave`；加入请求在途时等其完成后立即补发 `leave`，避免 join/leave 乱序。
- **收到新频道首包才切换**：选台只把机顶盒挂入目标频道的等待集合；UDP 首包到达时才迁移收看关系。
- **加入失败继续收旧台**：上游拒绝/不可达时，等待中的机顶盒保持 `current_channel` 不变，`last_error` 记录原因。
- **等待中可改选/退订**：每次选台/退订递增机顶盒 `generation` 并把旧请求移出等待集合。
- **已取消的选台不因迟到首包生效**：首包到达时校验 `generation` 与 `pending_channel`，不一致直接丢弃该等待项。

### 数据面（读包缓存，本次优化）

- **节目包只存一份**：收到的包放入所属频道的共享缓存 `ChannelState.buffer`（带频道内序号与全局到达序号），
  不再按机顶盒逐份复制。
- **未读进度记录不随积压增长**：每台机顶盒只记录各频道的未读区间游标
  `StbState.windows`（频道 → `[start, end)` 区间列表；持续同看一个频道时恒为 1 段，
  换台再换回才增加段数），另有 `unread` 计数冗余。
- **按到包顺序读，读走只影响本机**：读包时按全局到达序号跨频道合并各频道的未读窗口，
  只推进本机游标；其他机顶盒的未读与共享缓存均不受影响。
- **MAX_QUEUE 限制本机全部未读包**（跨频道合计，默认 200）：到包使本机未读超限时，
  丢本机全局到达序号最早的包（推进本机最旧游标），不影响其他机顶盒。
- **换台/退订后旧包仍可读**：换台或退订只冻结旧频道的未读窗口，不清空；
  旧包保留到本机读走或被本机 MAX_QUEUE 挤掉为止，他人继续收看/读包都不会提前清掉。
- **缓存释放**：一个包对所有机顶盒都不再未读时从共享缓存头部裁掉；
  频道无人收看且没有待取的包时，该频道的节目包缓存整体释放（序号归零）。
- **接口格式与计数口径不变**：`GET /stbs/{sid}/packets` 仍返回
  `{stb, current_channel, returned, packets}`，包字段不变（含 `received_at`）；
  `status`/`/state` 的 `queued_packets` 仍为本机全部未读包数。
  `/state` 另新增观测项：每频道 `buffered_packets`（共享缓存占用）、每机顶盒 `tracked_windows`（未读进度记录条数）。

## 自测

```bash
python3 tests/selftest.py    # 需先启动 proxy 与 source
```

覆盖 11 个场景共 65 项断言：两台共用频道（一路订阅/分别退订）、延迟首包期间不切换、
等待中改选且迟到首包不生效、等待中退订、加入失败保旧台、两台同时等同一频道首包、
共享缓存只存一份且未读进度记录不随积压增长、快慢读互不影响（慢读者仍能读到同样的包）、
换台/退订后读取旧包且他人读包不会提前清掉、MAX_QUEUE 超限丢本机最旧包且不拖累快读者、
收尾无残留订阅与缓存。

## 验证结果

- 本沙箱无 Docker，`docker compose` 未实际运行；以裸进程方式启动同一套代码完成等价验证
  （compose 配置未改动，两个服务的镜像内容与裸跑进程一致）。
- `tests/selftest.py` 全部 65 项断言通过（连续 3 次运行均通过）。
- `/docs`（proxy:8000 与 source:9000）均返回 200；按上文步骤走通
  "两台同看频道 1 → stb-1 换台频道 2"演示，实测：两台各积压 5 包时频道缓存仅 5 份；
  stb-1 换台后读出 6 个频道 1 旧包 + 3 个频道 2 新包（旧台在前）；stb-2 全程不受影响。
- 缓存占用实测：MAX_QUEUE=200、发包间隔 0.05s 时，不读的机顶盒未读数顶到 200 后稳定，
  频道缓存同步稳定在 ≈200（而非按机顶盒数翻倍）；全部读净并退订后 `buffered_packets` 归零。

## 已知限制 / 未完成项

- 数据面用"UDP 单播转发"模拟组播，控制面用 HTTP 模拟 IGMP；接真实组播时需替换为
  `IP_ADD_MEMBERSHIP` 组播 socket 与 IGMP 交互。
- 机顶盒读包为轮询模式；可扩展 WebSocket/SSE 推送。
- 状态全在内存，单 worker 运行；代理重启即丢失，无鉴权、无持久化、无多副本一致性。
- 上游 `leave` 失败（源不可达）不重试，源侧可能短暂残留发包，代理会直接丢弃未订阅频道的包。
- 按需求，换台/退订后未读旧包须保留到本机读走或被本机 MAX_QUEUE 挤掉：若某机顶盒留下未读积压后
  不再读取也不再收包（彻底闲置），而频道仍有他人在看，它钉住的共享缓存前缀会随频道流媒体增长；
  真实部署可加"闲置机顶盒回收"策略（如超时清空其未读窗口）来兜底。
- 读包按全局到达序号逐包弹出合并，单台单次读取为 O(limit × 频道数)；频道数固定为 10，开销可忽略。
