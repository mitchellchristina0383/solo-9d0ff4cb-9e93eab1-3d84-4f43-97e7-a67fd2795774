"""端到端自测: 园区电视组播换台代理 + 模拟节目源.

前置: 代理 (默认 http://127.0.0.1:8000) 与模拟源 (默认 http://127.0.0.1:9000) 已启动.

覆盖场景:
  1. 两台机顶盒共用同一频道 -> 上游只一路订阅; 都能收包; 全部退订后上游退订
  2. 首包延迟期间不切换, 首包到达才切换
  3. 等待首包时改选 -> 已取消的选台不因迟到的首包生效
  4. 等待首包时退订 -> 上游不再发该频道包
  5. 加入失败 -> 继续收旧台
  6. 两台同时等待同一频道首包 -> 仍只一路订阅, 首包到达同时切换
  7. 共享缓存: 同看一个频道时节目包只存一份, 每台的未读进度记录不随积压增长
  8. 快慢读互不影响: 快读读走只影响本机, 慢读随后仍能读到同样的包; 读净后缓存释放
  9. 换台/退订后仍能读取旧包, 他人读包不会提前清掉; 跨频道按到包顺序返回
 10. MAX_QUEUE 限制本机全部未读包, 超限丢本机最早的包, 不影响其他机顶盒
 11. 结束时不存在残留的上游订阅与节目包缓存
"""

from __future__ import annotations

import os
import sys
import time

import httpx

PROXY = os.getenv("PROXY_URL", "http://127.0.0.1:8000")
SOURCE = os.getenv("SOURCE_URL", "http://127.0.0.1:9000")

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def wait_until(fn, timeout: float = 6.0, interval: float = 0.1) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except httpx.HTTPError:
            pass
        time.sleep(interval)
    return False


def select(sid: str, channel: int) -> dict:
    r = httpx.post(f"{PROXY}/stbs/{sid}/select", json={"channel": channel}, timeout=5)
    r.raise_for_status()
    return r.json()


def unsubscribe(sid: str) -> dict:
    r = httpx.post(f"{PROXY}/stbs/{sid}/unsubscribe", timeout=5)
    r.raise_for_status()
    return r.json()


def status(sid: str) -> dict:
    return httpx.get(f"{PROXY}/stbs/{sid}/status", timeout=5).json()


def drain(sid: str) -> list[dict]:
    return httpx.get(f"{PROXY}/stbs/{sid}/packets", params={"limit": 500}, timeout=5).json()["packets"]


def source_state() -> dict:
    return httpx.get(f"{SOURCE}/state", timeout=5).json()


def proxy_state() -> dict:
    return httpx.get(f"{PROXY}/state", timeout=5).json()


def drain_aligned(*sids: str) -> None:
    """反复清空各台未读, 直到同一轮内都没有新包 (对齐各台未读起点)."""
    for _ in range(50):
        if all(not drain(s) for s in sids):
            return
    raise AssertionError(f"无法对齐 {sids} 的未读起点")


def source_config(channel: int, **kw) -> None:
    httpx.post(f"{SOURCE}/config/{channel}", json=kw, timeout=5).raise_for_status()


def main() -> int:
    httpx.post(f"{SOURCE}/reset", timeout=5)
    httpx.post(f"{PROXY}/reset", timeout=5)

    # ---------------- 场景 1: 两台共用频道 ----------------
    print("\n== 场景 1: 两台机顶盒共用同一频道 ==")
    select("stb-a", 1)
    select("stb-b", 1)
    check("stb-a 切换到频道 1", wait_until(lambda: status("stb-a")["current_channel"] == 1))
    check("stb-b 切换到频道 1", wait_until(lambda: status("stb-b")["current_channel"] == 1))
    st = source_state()["1"]
    check("上游对频道 1 只有一路订阅", st["joins"] == 1 and st["subscribed"], str(st))
    time.sleep(1.2)
    pa, pb = drain("stb-a"), drain("stb-b")
    check("stb-a 收到频道 1 节目包", any(p["channel"] == 1 for p in pa))
    check("stb-b 收到频道 1 节目包", any(p["channel"] == 1 for p in pb))
    unsubscribe("stb-a")
    time.sleep(0.3)
    check("stb-a 退订后上游仍保留订阅 (stb-b 在看)", source_state()["1"]["subscribed"])
    unsubscribe("stb-b")
    check("无人收看后上游退订频道 1",
          wait_until(lambda: not source_state()["1"]["subscribed"]))
    check("上游只收到一次 leave", source_state()["1"]["leaves"] == 1, str(source_state()["1"]))

    # ---------------- 场景 2: 首包延迟, 到包才切换 ----------------
    print("\n== 场景 2: 延迟首包, 收到首包才切换 ==")
    source_config(2, delay=2.0, interval=3.0)
    select("stb-d", 2)
    s = status("stb-d")
    check("选台后进入等待首包状态", s["pending_channel"] == 2 and s["current_channel"] is None, str(s))
    time.sleep(1.0)
    s = status("stb-d")
    check("首包未到不切换", s["current_channel"] is None and s["pending_channel"] == 2, str(s))
    check("首包到达后切换到频道 2",
          wait_until(lambda: status("stb-d")["current_channel"] == 2, timeout=4.0))

    # ---------------- 场景 3: 等待中改选, 迟到首包不生效 ----------------
    print("\n== 场景 3: 等待首包时改选, 已取消的选台不因迟到首包生效 ==")
    select("stb-c", 1)
    wait_until(lambda: status("stb-c")["current_channel"] == 1)
    drain("stb-c")
    joins_before = source_state()["2"]["joins"]
    select("stb-c", 2)  # 频道 2 已在播 (stb-d), 无需新订阅
    s = status("stb-c")
    check("改选频道 2 进入等待首包", s["pending_channel"] == 2, str(s))
    select("stb-c", 3)  # 等待中再次改选 -> 取消对频道 2 的请求
    s = status("stb-c")
    # 频道 3 无首包延迟, 查询时可能已切换; 关键是绝不在频道 2 上
    check("等待中改选频道 3 (未落到频道 2)",
          (s["pending_channel"] == 3 or s["current_channel"] == 3)
          and s["current_channel"] != 2, str(s))
    check("频道 2 未发起新的上游订阅", source_state()["2"]["joins"] == joins_before)
    check("stb-c 切换到频道 3", wait_until(lambda: status("stb-c")["current_channel"] == 3, timeout=3.0))
    # 等频道 2 的下一包 (interval=3s) 到达代理: 若已取消的请求生效, stb-c 会被切到 2
    check("频道 2 的迟到首包已到达代理",
          wait_until(lambda: source_state()["2"]["seq"] >= 2, timeout=6.0))
    time.sleep(0.3)
    s = status("stb-c")
    check("迟到的频道 2 首包未使已取消的选台生效", s["current_channel"] == 3, str(s))
    pkts = drain("stb-c")
    chans = {p["channel"] for p in pkts}
    check("stb-c 只收到旧台(1)与新台(3)的包, 无频道 2 的包",
          2 not in chans and 1 in chans and 3 in chans, f"channels={chans}")
    check("stb-d 仍在收看频道 2", status("stb-d")["current_channel"] == 2)
    check("stb-d 持续收到频道 2 的包", any(p["channel"] == 2 for p in drain("stb-d")))

    # ---------------- 场景 4: 等待首包时退订 ----------------
    print("\n== 场景 4: 等待首包时退订 ==")
    source_config(4, delay=2.0)
    select("stb-e", 1)
    wait_until(lambda: status("stb-e")["current_channel"] == 1)
    select("stb-e", 4)
    s = status("stb-e")
    check("选台频道 4 进入等待首包", s["pending_channel"] == 4, str(s))
    unsubscribe("stb-e")
    s = status("stb-e")
    check("等待中退订成功", s["current_channel"] is None and s["pending_channel"] is None, str(s))
    check("上游已退订频道 4",
          wait_until(lambda: source_state()["4"]["leaves"] >= 1 and not source_state()["4"]["subscribed"]))
    time.sleep(2.2)  # 超过首包延迟
    st = source_state()["4"]
    check("频道 4 首包始终未发出", st["seq"] == 0, str(st))
    check("stb-e 未收到频道 4 的包", not any(p["channel"] == 4 for p in drain("stb-e")))

    # ---------------- 场景 5: 加入失败继续收旧台 ----------------
    print("\n== 场景 5: 加入被拒绝, 继续收旧台 ==")
    source_config(9, refuse=True)
    select("stb-f", 1)
    wait_until(lambda: status("stb-f")["current_channel"] == 1)
    drain("stb-f")
    select("stb-f", 9)
    check("选台失败已回报",
          wait_until(lambda: status("stb-f")["last_error"] is not None, timeout=4.0))
    s = status("stb-f")
    check("加入失败后仍停留在旧台", s["current_channel"] == 1 and s["pending_channel"] is None, str(s))
    check("错误信息可见", "failed" in (s["last_error"] or ""), str(s["last_error"]))
    time.sleep(1.2)
    pkts = drain("stb-f")
    check("继续收到旧台(频道 1)的包", any(p["channel"] == 1 for p in pkts))
    check("未收到频道 9 的包", not any(p["channel"] == 9 for p in pkts))
    st = source_state()["9"]
    check("上游未建立频道 9 订阅", not st["subscribed"] and st["joins"] == 0, str(st))

    # ---------------- 场景 6: 两台同时等同一频道首包 ----------------
    print("\n== 场景 6: 两台同时等待同一频道首包, 仍只一路订阅 ==")
    source_config(5, delay=1.0)
    select("stb-g", 5)
    select("stb-h", 5)
    time.sleep(0.3)
    st = source_state()["5"]
    check("等待期间上游只有一路订阅", st["joins"] == 1, str(st))
    check("stb-g 切换到频道 5", wait_until(lambda: status("stb-g")["current_channel"] == 5, timeout=4.0))
    check("stb-h 切换到频道 5", wait_until(lambda: status("stb-h")["current_channel"] == 5, timeout=4.0))
    time.sleep(1.2)
    check("stb-g 收到频道 5 的包", any(p["channel"] == 5 for p in drain("stb-g")))
    check("stb-h 收到频道 5 的包", any(p["channel"] == 5 for p in drain("stb-h")))
    unsubscribe("stb-g")
    unsubscribe("stb-h")
    check("全部退订后上游退订频道 5",
          wait_until(lambda: not source_state()["5"]["subscribed"]))

    # ---------------- 场景 7: 共享缓存只存一份 ----------------
    print("\n== 场景 7: 同看一个频道, 节目包只缓存一份, 未读进度记录不随积压增长 ==")
    source_config(6, interval=0.5)  # 先慢速发包, 便于对齐两台未读起点
    select("stb-r1", 6)
    select("stb-r2", 6)
    check("stb-r1 切换到频道 6", wait_until(lambda: status("stb-r1")["current_channel"] == 6))
    check("stb-r2 切换到频道 6", wait_until(lambda: status("stb-r2")["current_channel"] == 6))
    drain_aligned("stb-r1", "stb-r2")  # 对齐两台未读起点
    source_config(6, interval=0.1)
    time.sleep(2.0)  # 两台都不读, 各积压约 20 个包
    s1, s2 = status("stb-r1"), status("stb-r2")
    n1, n2 = s1["queued_packets"], s2["queued_packets"]
    buf6 = proxy_state()["channels"]["6"]["buffered_packets"]  # 读状态期间仍陆续来包, 允许少量误差
    check("两台各自积压未读包", n1 >= 5 and n2 >= 5, f"r1={n1} r2={n2}")
    stbs_st = proxy_state()["stbs"]
    check("每台未读进度记录条数不随积压增长 (各 1 段)",
          stbs_st["stb-r1"]["tracked_windows"] == 1 and stbs_st["stb-r2"]["tracked_windows"] == 1,
          f"r1={stbs_st['stb-r1']['tracked_windows']} r2={stbs_st['stb-r2']['tracked_windows']}")
    check("频道缓存只存一份 (≈单机未读数, 而非两台之和)",
          buf6 <= max(n1, n2) + 5 and buf6 < n1 + n2,
          f"buffered={buf6} r1={n1} r2={n2}")

    # ---------------- 场景 8: 快慢读互不影响, 读净后缓存释放 ----------------
    print("\n== 场景 8: 快读读走只影响本机, 慢读仍能读到同样的包 ==")
    p1 = drain("stb-r1")  # 快读者读走全部积压
    check("stb-r1 (快) 读走全部积压", len(p1) >= 5, f"got {len(p1)}")
    check("stb-r1 读走后 stb-r2 的未读不丢",
          status("stb-r2")["queued_packets"] >= n2, f"r2={status('stb-r2')['queued_packets']} 之前={n2}")
    p2 = drain("stb-r2")  # 慢读者随后读
    seqs1, seqs2 = [p["seq"] for p in p1], [p["seq"] for p in p2]
    check("stb-r2 (慢) 仍能读到 stb-r1 读过的同样的包",
          seqs2[: len(seqs1)] == seqs1, f"r1={seqs1} r2={seqs2}")
    check("慢读到的包按到包顺序 (seq 递增)", seqs2 == sorted(seqs2), f"r2={seqs2}")
    unsubscribe("stb-r1")
    unsubscribe("stb-r2")
    drain("stb-r1")
    drain("stb-r2")
    check("无人收看且没有待取的包后缓存释放",
          wait_until(lambda: proxy_state()["channels"]["6"]["buffered_packets"] == 0))
    check("无人收看后上游退订频道 6",
          wait_until(lambda: not source_state()["6"]["subscribed"]))

    # ---------------- 场景 9: 换台/退订后仍能读取旧包 ----------------
    print("\n== 场景 9: 换台/退订后仍能读取旧包, 他人读包不会提前清掉 ==")
    unsubscribe("stb-f")  # 场景 5 的 stb-f 仍在看频道 1, 先清场以免影响缓存释放检查
    for sid in ("stb-a", "stb-b", "stb-e", "stb-f"):
        drain(sid)  # 清掉各台在频道 1 的残余未读旧包 (它们按规定仍可读, 不清会钉住共享缓存)
    select("stb-s1", 1)
    select("stb-s2", 1)
    wait_until(lambda: status("stb-s1")["current_channel"] == 1)
    wait_until(lambda: status("stb-s2")["current_channel"] == 1)
    drain("stb-s1")
    drain("stb-s2")
    time.sleep(1.2)  # 频道 1 积压约 2 包
    select("stb-s1", 3)  # stb-s1 换台 (频道 3 无首包延迟)
    check("stb-s1 换到频道 3", wait_until(lambda: status("stb-s1")["current_channel"] == 3))
    time.sleep(0.8)  # 新频道积压
    drain("stb-s2")  # 他人读走频道 1 的包, 不应清掉 stb-s1 的旧包
    check("他人读包后 stb-s1 的旧包仍留在缓存",
          proxy_state()["channels"]["1"]["buffered_packets"] > 0,
          f"buffered={proxy_state()['channels']['1']['buffered_packets']}")
    pkts = drain("stb-s1")
    idx1 = [i for i, p in enumerate(pkts) if p["channel"] == 1]
    idx3 = [i for i, p in enumerate(pkts) if p["channel"] == 3]
    check("换台后仍能读到旧频道 1 的未读包", len(idx1) >= 1, f"ch1={len(idx1)}")
    check("同时收到新频道 3 的包", len(idx3) >= 1, f"ch3={len(idx3)}")
    check("跨频道按到包顺序返回 (旧台包在前)",
          bool(idx1) and bool(idx3) and max(idx1) < min(idx3),
          f"idx1={idx1} idx3={idx3}")
    check("包字段保持原格式 (含 received_at/seq/payload)",
          all("received_at" in p and "seq" in p and "payload" in p for p in pkts))
    time.sleep(0.8)
    unsubscribe("stb-s2")  # 退订时频道 1 尚有未读包
    pkts2 = drain("stb-s2")
    check("退订后仍能读取退订前的未读旧包", any(p["channel"] == 1 for p in pkts2),
          f"got {len(pkts2)}")
    unsubscribe("stb-s1")
    drain("stb-s1")
    check("旧包读净后频道 1 缓存释放",
          wait_until(lambda: proxy_state()["channels"]["1"]["buffered_packets"] == 0))

    # ---------------- 场景 10: MAX_QUEUE 超限丢本机最早的包 ----------------
    print("\n== 场景 10: MAX_QUEUE 限制本机未读, 超限丢最旧, 不影响他机 ==")
    max_queue = int(os.getenv("PROXY_MAX_QUEUE", "200"))
    source_config(7, interval=0.05)
    select("stb-q1", 7)
    select("stb-q2", 7)
    wait_until(lambda: status("stb-q1")["current_channel"] == 7)
    wait_until(lambda: status("stb-q2")["current_channel"] == 7)
    drain("stb-q1")
    drain("stb-q2")
    # stb-q1 不读, 等积压顶到上限; stb-q2 持续快读验证不受拖累
    q2_seqs: list[int] = []
    deadline = time.time() + 25.0
    capped = False
    while time.time() < deadline:
        q2_seqs.extend(p["seq"] for p in drain("stb-q2"))
        if status("stb-q1")["queued_packets"] >= max_queue:
            capped = True
            break
        time.sleep(0.5)
    check(f"stb-q1 未读数顶到上限 MAX_QUEUE={max_queue}", capped,
          f"queued={status('stb-q1')['queued_packets']}")
    time.sleep(1.0)  # 继续来包, 触发溢出丢弃
    q2_seqs.extend(p["seq"] for p in drain("stb-q2"))
    check("未读数保持上限不再增长", status("stb-q1")["queued_packets"] == max_queue,
          f"queued={status('stb-q1')['queued_packets']}")
    pkts = drain("stb-q1")
    seqs = [p["seq"] for p in pkts]
    check("读出数量等于上限", len(pkts) == max_queue, f"got {len(pkts)}")
    check("最旧的包已被丢弃 (最早 seq 已后移)", bool(seqs) and seqs[0] > 1,
          f"first seq={seqs[0] if seqs else None}")
    check("剩余包连续无空洞", seqs == list(range(seqs[0], seqs[0] + len(seqs))),
          f"head={seqs[:3]} tail={seqs[-3:]}")
    check("频道缓存同样只有一份 (≈上限而非两倍)",
          proxy_state()["channels"]["7"]["buffered_packets"] <= max_queue + 5,
          f"buffered={proxy_state()['channels']['7']['buffered_packets']}")
    check("快读的 stb-q2 不受 stb-q1 溢出拖累 (seq 连续无丢失)",
          q2_seqs == list(range(q2_seqs[0], q2_seqs[0] + len(q2_seqs))),
          f"got {len(q2_seqs)} pkts, head={q2_seqs[:3]}")
    unsubscribe("stb-q1")
    unsubscribe("stb-q2")
    drain("stb-q1")
    drain("stb-q2")

    # ---------------- 场景 11: 无残留订阅与缓存 ----------------
    print("\n== 场景 11: 收尾检查 ==")
    for sid in ("stb-c", "stb-d", "stb-e", "stb-f"):
        unsubscribe(sid)
    check("所有频道上游均已退订",
          wait_until(lambda: all(not c["subscribed"] for c in source_state().values()), timeout=4.0))
    for sid in ("stb-a", "stb-b", "stb-c", "stb-d", "stb-e", "stb-f", "stb-g", "stb-h"):
        drain(sid)  # 读净各台残余未读, 缓存应全部释放
    check("所有频道缓存已释放",
          wait_until(lambda: all(c["buffered_packets"] == 0
                                 for c in proxy_state()["channels"].values()), timeout=4.0),
          str({n: c["buffered_packets"] for n, c in proxy_state()["channels"].items()}))

    print()
    if failures:
        print(f"共 {len(failures)} 项失败: {failures}")
        return 1
    print("全部自测通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
