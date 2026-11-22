"""园区电视组播换台代理 (FastAPI).

机顶盒 (STB) API:
  POST /stbs/{sid}/select       按频道号选台 (202: 等待首包; 200: 已在收看)
  POST /stbs/{sid}/unsubscribe  退订
  GET  /stbs/{sid}/packets      读取本机未读的节目包 (按到包顺序, 读走只影响本机)
  GET  /stbs/{sid}/status       当前频道 / 等待中的频道 / 最近错误 / 未读包数
  GET  /state                   代理内部状态 (调试用, 含各频道共享缓存占用)

核心规则:
  - 同一频道只向上游订阅一路 (收看者与等待首包者共享)
  - 无人收看且无人等候首包时退订上游
  - 收到新频道首包才切换; 加入失败就继续收旧台
  - 等待首包时仍可改选或退订; 已取消的选台请求不因迟到的首包生效
    (每次选台/退订递增机顶盒代次 generation, 首包到达时校验)

读包缓存 (每频道一份共享缓存 + 每机顶盒游标):
  - 收到的每个节目包只在内存里存一份, 放在所属频道的 ChannelState.buffer,
    按频道内序号排列, 并带全局到达序号 (跨频道按到包顺序合并用)
  - 每台机顶盒只记录自己在各频道的未读区间 (StbState.windows: 频道 -> [start, end) 列表),
    多台同看一个频道时, 每台记录未读进度的空间不随积压包数增长
  - 各台按到包顺序读; 读走只推进本机游标, 不影响其他机顶盒
  - MAX_QUEUE 限制本机全部未读包 (跨频道合计), 超限丢本机最早的包
  - 换台/退订后旧频道的未读窗口保留, 旧包仍可读; 他人继续收看或读包都不会提前清掉
  - 一个包对所有机顶盒都不再未读时, 从共享缓存头部裁掉;
    频道无人收看且没有待取的包时, 该频道的节目包缓存整体释放
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

SOURCE_URL = os.getenv("SOURCE_URL", "http://127.0.0.1:9000").rstrip("/")
UDP_LISTEN_PORT = int(os.getenv("UDP_LISTEN_PORT", "5000"))
CHANNEL_MIN = int(os.getenv("CHANNEL_MIN", "1"))
CHANNEL_MAX = int(os.getenv("CHANNEL_MAX", "10"))
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "200"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "5.0"))


class ChannelState:
    """一个频道的代理侧状态 (含全频道共享的节目包缓存)."""

    def __init__(self, number: int):
        self.number = number
        self.subscribed = False  # 上游订阅已建立
        self.join_task: asyncio.Task | None = None  # 进行中的上游加入任务
        self.watchers: set[str] = set()  # 正在收看的机顶盒
        self.pending: dict[str, int] = {}  # 等待首包的机顶盒 -> 选台代次
        # 共享节目包缓存: 每个包只存一份, 元素为 (全局到达序号, 包字典)
        # buffer[i] 的频道内序号为 base + i; 已无任何机顶盒未读的包从左侧裁掉
        self.buffer: deque[tuple[int, dict]] = deque()
        self.base = 0  # buffer 首包的频道内序号
        self.next_seq = 0  # 下一个待分配的频道内序号


class StbState:
    """一台机顶盒的状态."""

    def __init__(self, sid: str):
        self.sid = sid
        self.current: int | None = None  # 当前收看频道
        self.pending_channel: int | None = None  # 等待首包的目标频道
        self.generation = 0  # 每次选台/退订 +1, 使已取消的请求失效
        self.last_error: str | None = None
        # 各频道的未读区间: 频道号 -> [[start, end), ...] (频道内序号, 升序且不重叠;
        # 换台再换回会产生多个区间). 只记录游标进度, 空间不随积压包数增长
        self.windows: dict[int, list[list[int]]] = {}
        self.unread = 0  # 本机全部未读包数 (= 所有窗口长度之和)


channels: dict[int, ChannelState] = {}
stbs: dict[str, StbState] = {}
http: httpx.AsyncClient | None = None
arrival_counter = 0  # 全局到包序号, 用于跨频道按到包顺序合并


# ---------------------------------------------------------------- 上游交互


async def _leave_upstream(number: int) -> None:
    try:
        assert http is not None
        await http.post(f"{SOURCE_URL}/leave/{number}")
    except httpx.HTTPError:
        pass  # 上游暂时不可达; 模拟源侧包会自行发完即被忽略


def _maybe_leave(cs: ChannelState) -> None:
    """无人收看且无人等候首包时退订上游."""
    if cs.watchers or cs.pending:
        return
    if cs.join_task is not None:
        return  # 加入请求在途: 由 _join_upstream 完成后兜底退订, 避免 join/leave 乱序
    if cs.subscribed:
        cs.subscribed = False
        asyncio.get_running_loop().create_task(_leave_upstream(cs.number))


def _ensure_join(cs: ChannelState) -> None:
    """同一频道只向上游订阅一路."""
    if cs.subscribed or cs.join_task is not None:
        return
    cs.join_task = asyncio.get_running_loop().create_task(_join_upstream(cs))


async def _join_upstream(cs: ChannelState) -> None:
    try:
        assert http is not None
        resp = await http.post(f"{SOURCE_URL}/join/{cs.number}")
        resp.raise_for_status()
    except Exception as exc:  # 加入被拒绝或上游不可达
        cs.join_task = None
        if isinstance(exc, httpx.HTTPStatusError):
            reason = f"upstream rejected join with {exc.response.status_code}"
        else:
            reason = f"upstream join error: {type(exc).__name__}"
        # 等待中的机顶盒选台失败, 继续收旧台
        for sid, gen in list(cs.pending.items()):
            stb = stbs.get(sid)
            if stb is not None and stb.generation == gen and stb.pending_channel == cs.number:
                stb.pending_channel = None
                stb.last_error = f"join channel {cs.number} failed: {reason}"
                cs.pending.pop(sid, None)
        return
    cs.subscribed = True
    cs.join_task = None
    _maybe_leave(cs)  # 加入完成前所有人都取消了 -> 立即退订


# ---------------------------------------------------------------- 机顶盒操作


def _get_stb(sid: str) -> StbState:
    stb = stbs.get(sid)
    if stb is None:
        stb = stbs[sid] = StbState(sid)
    return stb


def _get_channel(number: int) -> ChannelState:
    cs = channels.get(number)
    if cs is None:
        cs = channels[number] = ChannelState(number)
    return cs


def _cancel_pending(stb: StbState) -> None:
    """取消等待中的选台; 之后迟到的首包对该请求不再生效."""
    if stb.pending_channel is not None:
        cs = channels.get(stb.pending_channel)
        if cs is not None:
            cs.pending.pop(stb.sid, None)
            _maybe_leave(cs)
        stb.pending_channel = None


# ---------------------------------------------------------------- 读包缓存


def _trim_buffer(cs: ChannelState) -> None:
    """裁掉所有机顶盒都不再未读的缓存前缀; 无待取的包时整体释放缓存."""
    firsts = [s.windows[cs.number][0][0] for s in stbs.values() if s.windows.get(cs.number)]
    new_base = min(firsts) if firsts else cs.next_seq
    while cs.buffer and cs.base < new_base:
        cs.buffer.popleft()
        cs.base += 1
    if not cs.buffer and not firsts:
        # 频道没有待取的包: 释放节目包缓存, 序号归零 (窗口必然已空, 无悬挂引用)
        cs.base = 0
        cs.next_seq = 0


def _note_arrival(stb: StbState, number: int, idx: int) -> None:
    """把新到的包登记为本机未读 (扩展或新建未读窗口)."""
    ws = stb.windows.setdefault(number, [])
    if ws and ws[-1][1] == idx:
        ws[-1][1] = idx + 1  # 与上一窗口连续: 直接扩展
    else:
        ws.append([idx, idx + 1])  # 换台回来产生的新区间
    stb.unread += 1


def _oldest_channel(stb: StbState) -> int | None:
    """本机最早未读包所在的频道 (按全局到达序号比较各窗口首包)."""
    best: int | None = None
    best_arrival: int | None = None
    for number, ws in stb.windows.items():
        if not ws:
            continue
        cs = channels.get(number)
        if cs is None:
            continue
        i = ws[0][0] - cs.base
        if not 0 <= i < len(cs.buffer):
            continue  # 防御: 缓存已释放时窗口必然也已清空, 不会走到这里
        arrival = cs.buffer[i][0]
        if best_arrival is None or arrival < best_arrival:
            best, best_arrival = number, arrival
    return best


def _pop_oldest(stb: StbState) -> dict | None:
    """取出本机最早的一个未读包 (只推进本机游标, 不影响其他机顶盒)."""
    number = _oldest_channel(stb)
    if number is None:
        return None
    cs = channels[number]
    ws = stb.windows[number]
    pkt = cs.buffer[ws[0][0] - cs.base][1]
    ws[0][0] += 1
    stb.unread -= 1
    if ws[0][0] >= ws[0][1]:
        ws.pop(0)
        if not ws:
            del stb.windows[number]
    _trim_buffer(cs)
    return pkt


def _enforce_quota(stb: StbState) -> None:
    """MAX_QUEUE 限制本机全部未读包, 超限丢本机最早的包."""
    while stb.unread > MAX_QUEUE:
        if _pop_oldest(stb) is None:
            stb.unread = 0  # 防御: 计数与窗口不一致时复位
            break


# ---------------------------------------------------------------- 数据面


def handle_packet(pkt: dict) -> None:
    """收到节目源转发来的一个节目包 (在事件循环内调用)."""
    global arrival_counter
    try:
        number = int(pkt.get("channel"))
    except (TypeError, ValueError):
        return
    cs = channels.get(number)
    if cs is None:
        return  # 未订阅的频道: 忽略

    # 共享缓存只存一份
    stored = dict(pkt)
    stored["received_at"] = round(time.time(), 3)
    idx = cs.next_seq
    cs.next_seq += 1
    arrival_counter += 1
    cs.buffer.append((arrival_counter, stored))

    # 首包到达: 把仍在等待该频道的机顶盒切换过来
    for sid, gen in list(cs.pending.items()):
        stb = stbs.get(sid)
        if stb is None or stb.generation != gen or stb.pending_channel != number:
            # 已改选/退订的请求不因迟到的首包生效
            cs.pending.pop(sid, None)
            continue
        old = stb.current
        if old is not None and old != number:
            old_cs = channels.get(old)
            if old_cs is not None:
                old_cs.watchers.discard(sid)
                _maybe_leave(old_cs)
        stb.current = number
        stb.pending_channel = None
        cs.pending.pop(sid, None)
        cs.watchers.add(sid)

    # 登记到各收看者的未读进度 (刚切换过来的从本包开始), 并按本机上限裁剪
    for sid in cs.watchers:
        stb = stbs.get(sid)
        if stb is None:
            continue
        _note_arrival(stb, number, idx)
        _enforce_quota(stb)

    # 无人未读的包 (如退订后迟到的包) 立即释放
    _trim_buffer(cs)


class _UdpProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        try:
            pkt = json.loads(data.decode())
        except (ValueError, UnicodeDecodeError):
            return
        if isinstance(pkt, dict):
            handle_packet(pkt)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT)
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        _UdpProtocol, local_addr=("0.0.0.0", UDP_LISTEN_PORT)
    )
    yield
    transport.close()
    await http.aclose()


app = FastAPI(
    title="Campus TV Multicast Switching Proxy",
    description="园区电视组播换台代理: 机顶盒选台/退订/读包, 上游每频道一路订阅, 节目包每频道只缓存一份.",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:
    return RedirectResponse("/docs")


# ---------------------------------------------------------------- REST API


class SelectRequest(BaseModel):
    channel: int


@app.post("/stbs/{sid}/select", status_code=202)
async def select(sid: str, body: SelectRequest) -> dict:
    """按频道号选台. 收到新频道首包后才真正切换."""
    number = body.channel
    if not (CHANNEL_MIN <= number <= CHANNEL_MAX):
        raise HTTPException(404, f"unknown channel {number} (valid {CHANNEL_MIN}..{CHANNEL_MAX})")
    stb = _get_stb(sid)
    stb.generation += 1  # 使此前未完成的选台请求失效
    stb.last_error = None
    _cancel_pending(stb)
    if stb.current == number:
        return {"stb": sid, "status": "watching", "channel": number, "generation": stb.generation}
    cs = _get_channel(number)
    cs.pending[sid] = stb.generation
    stb.pending_channel = number
    _ensure_join(cs)
    return {
        "stb": sid,
        "status": "pending_first_packet",
        "channel": number,
        "generation": stb.generation,
    }


@app.post("/stbs/{sid}/unsubscribe")
async def unsubscribe(sid: str) -> dict:
    """退订: 停止收看并取消等待中的选台. 未读旧包仍保留, 可继续读取."""
    stb = _get_stb(sid)
    stb.generation += 1
    _cancel_pending(stb)
    if stb.current is not None:
        cs = channels.get(stb.current)
        if cs is not None:
            cs.watchers.discard(sid)
            _maybe_leave(cs)
        stb.current = None
    return {"stb": sid, "status": "unsubscribed"}


@app.get("/stbs/{sid}/packets")
async def packets(sid: str, limit: int = 100) -> dict:
    """按到包顺序读取本机未读的节目包 (读走只推进本机游标)."""
    stb = _get_stb(sid)
    limit = max(1, min(limit, 500))
    out: list[dict] = []
    for _ in range(limit):
        pkt = _pop_oldest(stb)
        if pkt is None:
            break
        out.append(pkt)
    return {
        "stb": sid,
        "current_channel": stb.current,
        "returned": len(out),
        "packets": out,
    }


@app.get("/stbs/{sid}/status")
async def status(sid: str) -> dict:
    stb = _get_stb(sid)
    return {
        "stb": sid,
        "current_channel": stb.current,
        "pending_channel": stb.pending_channel,
        "generation": stb.generation,
        "last_error": stb.last_error,
        "queued_packets": stb.unread,
    }


@app.get("/stbs")
async def list_stbs() -> dict:
    return {sid: {"current": s.current, "pending": s.pending_channel} for sid, s in stbs.items()}


@app.get("/state")
async def debug_state() -> dict:
    return {
        "channels": {
            str(n): {
                "upstream_subscribed": cs.subscribed,
                "join_in_progress": cs.join_task is not None,
                "watchers": sorted(cs.watchers),
                "pending": sorted(cs.pending),
                "buffered_packets": len(cs.buffer),  # 共享缓存占用的包数 (全频道仅一份)
            }
            for n, cs in sorted(channels.items())
        },
        "stbs": {
            sid: {
                "current_channel": s.current,
                "pending_channel": s.pending_channel,
                "generation": s.generation,
                "last_error": s.last_error,
                "queued_packets": s.unread,
                "tracked_windows": sum(len(w) for w in s.windows.values()),  # 未读进度记录条数
            }
            for sid, s in sorted(stbs.items())
        },
    }


@app.post("/reset")
async def reset() -> dict:
    """清空全部机顶盒并退订所有上游频道 (演示/自测用)."""
    global arrival_counter
    for cs in channels.values():
        if cs.join_task is not None:
            cs.join_task.cancel()
            cs.join_task = None
        if cs.subscribed:
            cs.subscribed = False
            await _leave_upstream(cs.number)
    channels.clear()
    stbs.clear()
    arrival_counter = 0
    return {"reset": True}
