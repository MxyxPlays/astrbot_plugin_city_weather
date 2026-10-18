# -*- coding: utf-8 -*-
"""
城市天气 · AstrBot 插件（高德天气 API）
================================================
支持全国城市（省 / 市 / 区县，可精确到县级市），用法（直接说话就行，不用记斜杠指令）：

    北京天气
    上海天气怎么样
    上海天气预报
    明天杭州天气
    温州瑞安天气
    天气 上海
    /天气 上海

原理：
    1. 从你说的话里抠出城市名（正则）
    2. 用高德「地理编码」把城市名换成 adcode（区号），
       因为天气接口只认 adcode，直接传「温州瑞安」是查不到的
    3. 用 adcode 调天气接口，实况用 extensions=base，预报用 extensions=all
    4. 把 JSON 拼成好读的中文发出去

注意：所有请求都是异步的（aiohttp），不要换成 requests，会卡死机器人。
"""

import asyncio
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp

from astrbot.api.all import Star, Context, AstrMessageEvent, Plain, MessageChain
from astrbot.api.event import filter
from astrbot.api import logger

API_ROOT = "https://restapi.amap.com"

# 城市名：2~15 个中英文字符（带「·」是兼容少数民族地名，如「巴音郭楞·库尔勒」）
CITY_RE = r"[\u4e00-\u9fa5A-Za-z·]{2,15}"

# 时间词，用来判断用户想问「今天 / 明天 / 未来几天」
TIME_WORDS = ("大后天", "今天", "明天", "后天", "未来几天", "未来三天", "这周", "本周")

# 形式一：城市名 + 天气（用户最常用）
#   温州瑞安天气 / 上海天气怎么样 / 北京天气预报 / 上海明天天气
SUFFIX_PATTERN = re.compile(
    r"^(?:帮我|帮忙|查一下|查下|查查|看看|查询)?"
    r"(?:大后天|今天|明天|后天|未来几天|未来三天|这周|本周)?"
    rf"(?P<city>{CITY_RE})"
    r"\s*"
    r"(?:大后天|今天|明天|后天)?"
    r"\s*(?:天气|气温|温度)"
    r"(?:预报|怎么样|咋样|如何|情况|怎样)?"
    r"[?？!！。，,~～\s]*$"
)

# 形式二：天气 + 城市（要带空格，避免「天气真好」这种闲聊被误触发）
PREFIX_PATTERN = re.compile(
    r"^/?\s*(?:天气|天气预报|气温|温度)\s+(?P<city>" + CITY_RE + r")[?？!！。，,~～\s]*$"
)

# 形式三：只有「天气」两个字 → 查默认城市
EXACT_PATTERN = re.compile(r"^/?\s*(?:天气|天气预报|气温|温度)[?？!！。~～\s]*$")

# 星期几：高德返回 1~7，7 是周日
WEEK_MAP = {"1": "周一", "2": "周二", "3": "周三", "4": "周四", "5": "周五", "6": "周六", "7": "周日"}

# 这几个错误码是「问得太快」，等一下重试就能成功
TRANSIENT_CODES = {"10004", "10021"}
MIN_REQUEST_INTERVAL = 0.4

# 预报分成多条发时，两条消息之间的间隔（秒）
SPLIT_DELAY = 1.2

ERROR_HINTS = {
    "10001": "API Key 无效（10001）：Key 的『服务平台』要选「Web服务」。",
    "10003": "今日调用量已超限（10003）：明天再试或去控制台提额。",
    "10004": "访问过于频繁（10004）：歇几秒再试。",
    "10009": "Key 与平台不匹配（10009）：这个 Key 不是「Web服务」类型。",
    "10012": "Key 已过期或被删除（10012）。",
    "10021": "QPS 超限（10021）：同时请求太多，稍后再试。",
    "10023": "调用量超限（10023）：免费额度用完了，下个月 1 号重置。",
    "10029": "Key 被禁用（10029）。",
}


class AmapWeather(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        conf = config or {}
        self.api_key = str(conf.get("amap_api_key", "") or "").strip()
        self.default_city = str(conf.get("default_city", "上海") or "上海").strip()
        self.only_when_woken = bool(conf.get("only_when_woken", False))
        self.show_source = bool(conf.get("show_source", True))
        # 预报是否「每天单独一条消息」；默认 false = 合并成一条多行消息
        self.forecast_split = bool(conf.get("forecast_split", False))
        try:
            self.timeout = max(5, int(conf.get("timeout", 20) or 20))
        except (TypeError, ValueError):
            self.timeout = 20

        self._last_request_at = 0.0
        logger.info(
            "[天气] 插件已加载 | Key=%s | 默认城市=%s",
            "已配置" if self.api_key else "未配置",
            self.default_city,
        )

    # ==================================================================
    # 一、网络请求
    # ==================================================================

    async def _request(self, path: str, params: Dict[str, Any]) -> Tuple[Optional[dict], str]:
        """GET 请求，自带限速 + 重试。返回 (数据, 错误说明)。"""
        if not self.api_key:
            return None, (
                "还没有配置高德 API Key。\n"
                "打开 AstrBot 面板 → 插件 → 城市天气 → 配置，"
                "把 amap_api_key 填进去（服务平台要选「Web服务」）。"
            )

        query = dict(params)
        query["key"] = self.api_key
        last_err = "未知错误"

        for attempt in range(3):
            # 免费 Key 的 QPS 很低，两次请求之间留点空档
            gap = time.monotonic() - self._last_request_at
            if gap < MIN_REQUEST_INTERVAL:
                await asyncio.sleep(MIN_REQUEST_INTERVAL - gap)
            self._last_request_at = time.monotonic()

            try:
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(API_ROOT + path, params=query) as resp:
                        if resp.status != 200:
                            last_err = f"HTTP {resp.status}"
                            if resp.status < 500:
                                return None, last_err
                        else:
                            data = await resp.json(content_type=None)
                            if isinstance(data, dict):
                                code = str(data.get("infocode") or "")
                                if str(data.get("status")) == "0" and code in TRANSIENT_CODES and attempt < 2:
                                    last_err = ERROR_HINTS.get(code, f"高德错误 {code}")
                                    await asyncio.sleep(1.2 * (attempt + 1))
                                    continue
                            return data, ""
            except asyncio.TimeoutError:
                last_err = f"请求超时（{self.timeout} 秒）"
            except aiohttp.ClientError as e:
                last_err = f"网络错误：{type(e).__name__}"
            except Exception as e:  # noqa: BLE001
                last_err = f"{type(e).__name__}: {e}"

            if attempt < 2:
                await asyncio.sleep(0.8 * (attempt + 1))

        return None, last_err

    def _error_text(self, data: dict) -> str:
        code = str(data.get("infocode") or "")
        info = str(data.get("info") or "")
        return ERROR_HINTS.get(code) or f"高德返回错误：{info or '未知'}（{code or '空'}）"

    # ==================================================================
    # 二、城市 → adcode
    # ==================================================================

    async def _geocode(self, name: str) -> Optional[dict]:
        """地名 → adcode。返回 {adcode, city, district, address}。"""
        data, err = await self._request("/v3/geocode/geo", {"address": name})
        if not data:
            logger.warning("[天气] 地理编码失败: %s (%s)", name, err)
            return None
        if str(data.get("status")) != "1":
            logger.warning("[天气] 地理编码报错: %s", self._error_text(data))
            return None
        geocodes = data.get("geocodes") or []
        if not geocodes:
            return None
        g = geocodes[0]
        adcode = str(g.get("adcode") or "")
        if not adcode:
            return None
        return {
            "adcode": adcode,
            "city": g.get("city") if not isinstance(g.get("city"), list) else "",
            "district": g.get("district") if not isinstance(g.get("district"), list) else "",
            "address": g.get("formatted_address") or name,
        }

    async def _resolve_city(self, city: str) -> Tuple[str, str]:
        """把用户说的城市变成 (查询用的城市参数, 找不到时的提示名)。"""
        city = (city or "").strip()
        if not city:
            city = self.default_city
        # 直接给了 6 位 adcode
        if re.fullmatch(r"\d{6}", city):
            return city, city

        geo = await self._geocode(city)
        if geo:
            return geo["adcode"], city
        # 兜底：有些城市名高德天气接口本身能认（比如「上海」），再试一次原名
        return city, city

    # ==================================================================
    # 三、取天气
    # ==================================================================

    async def fetch_weather(self, city: str, want_forecast: bool) -> Tuple[Optional[dict], str]:
        city_key, shown_city = await self._resolve_city(city)
        data, err = await self._request(
            "/v3/weather/weatherInfo",
            {"city": city_key, "extensions": "all" if want_forecast else "base"},
        )
        if not data:
            return None, err
        if str(data.get("status")) != "1":
            return None, self._error_text(data)

        if want_forecast:
            forecasts = data.get("forecasts") or []
            if not forecasts:
                return None, f"没查到「{shown_city}」的预报，换个写法试试（比如「上海天气预报」「杭州西湖天气预报」）。"
            return forecasts[0], ""

        lives = data.get("lives") or []
        if not lives:
            return None, f"没查到「{shown_city}」的天气，换个写法试试（比如「上海天气」「北京天气」）。"
        return lives[0], ""

    # ==================================================================
    # 四、拼中文回复
    # ==================================================================

    def _source_line(self, report_time: str) -> str:
        """末尾的「分隔线 + 更新时间」。

        注意：这里刻意不写「数据来源：高德天气」，用户要求去掉。
        字符里也不能出现 ~ 。？ ！ … —— AstrBot 的「分段回复」会按这些符号
        把一条消息拆成好几条，导致数字被切断（如 24~ / 33℃）。
        """
        if not self.show_source:
            return ""
        t_raw = str(report_time or "")
        t = t_raw[-8:-3] if len(t_raw) >= 8 else ""
        return f"──────────\n🕐 更新 {t}" if t else ""

    def _with_source(self, text: str, report_time: str) -> str:
        src = self._source_line(report_time)
        return f"{text}\n{src}" if src else text

    def format_now(self, live: dict) -> str:
        name = live.get("city") or "未知城市"
        weather = live.get("weather") or "未知"
        temp = live.get("temperature") or "?"
        humidity = live.get("humidity") or "?"
        wind_dir = live.get("winddirection") or ""
        wind_power = live.get("windpower") or ""
        wind = f"{wind_dir}风 {wind_power}级" if wind_dir else "未知"
        text = (
            f"🌤 {name} 天气\n"
            f"──────────\n"
            f"天气：{weather}\n"
            f"气温：{temp}℃\n"
            f"湿度：{humidity}%\n"
            f"风力：{wind}"
        )
        return self._with_source(text, live.get("reporttime") or "")

    def _day_label(self, index: int) -> str:
        return {0: "今天", 1: "明天", 2: "后天", 3: "大后天"}.get(index, "")

    def format_cast_line(self, cast: dict, index: int) -> str:
        date = cast.get("date") or ""
        week = WEEK_MAP.get(str(cast.get("week") or ""), "")
        day_w = cast.get("dayweather") or ""
        night_w = cast.get("nightweather") or ""
        low = cast.get("nighttemp") or "?"
        high = cast.get("daytemp") or "?"
        label = self._day_label(index)
        head = f"{label}({date[5:]} {week})" if label else f"({date[5:]} {week})"
        # 白天和夜间天气一样时不要写两遍
        w_text = day_w if day_w == night_w else f"{day_w}转{night_w}"
        # 温度区间用「-」连接，不用「~」：~ 会被 AstrBot 的分段回复当成断句符号
        return f"{head} {w_text} {low}-{high}℃"

    def format_forecast(self, forecast: dict, day_index: Optional[int]) -> Union[str, List[str]]:
        name = forecast.get("city") or "未知城市"
        casts = forecast.get("casts") or []
        if not casts:
            return f"没拿到 {name} 的预报数据。"

        # 用户明确问「明天 / 后天」，就只发那一天
        if day_index is not None and 0 <= day_index < len(casts):
            c = casts[day_index]
            date = c.get("date") or ""
            week = WEEK_MAP.get(str(c.get("week") or ""), "")
            label = self._day_label(day_index) or "当天"
            text = (
                f"📅 {name} {label}（{date[5:]} {week}）\n"
                f"──────────\n"
                f"白天：{c.get('dayweather') or '未知'} {c.get('daytemp') or '?'}℃\n"
                f"夜间：{c.get('nightweather') or '未知'} {c.get('nighttemp') or '?'}℃\n"
                f"风：{c.get('daywind') or '未知'}风 {c.get('daypower') or '?'}级"
            )
            return self._with_source(text, forecast.get("reporttime") or "")

        casts = casts[:4]
        src = self._source_line(forecast.get("reporttime") or "")

        # 合并成一条（旧样式）
        if not self.forecast_split:
            lines = [f"📅 {name} 天气预报", "──────────"]
            for i, c in enumerate(casts):
                lines.append(self.format_cast_line(c, i))
            if src:
                lines.append(src)
            return "\n".join(lines)

        # 每天单独一条：标题+今天一条，之后每天一条
        parts = [
            f"📅 {name} 天气预报\n──────────\n{self.format_cast_line(casts[0], 0)}",
        ]
        for i, c in enumerate(casts[1:], start=1):
            parts.append(self.format_cast_line(c, i))
        if src:
            parts[-1] = f"{parts[-1]}\n{src}"
        return parts

    # ==================================================================
    # 五、识别意图 + 主流程
    # ==================================================================

    @staticmethod
    def _clean_city(raw: str) -> str:
        """把混进城市名里的「今天/明天」等词剥掉（可能在前面也可能在后面）。"""
        c = (raw or "").strip()
        changed = True
        while changed and c:
            changed = False
            for w in TIME_WORDS:
                if c.startswith(w):
                    c = c[len(w):].strip()
                    changed = True
                if c.endswith(w) and len(c) > len(w):
                    c = c[: -len(w)].strip()
                    changed = True
        return c.strip()

    @staticmethod
    def _day_index(text: str) -> Optional[int]:
        """判断用户问的是哪一天：今天=0，明天=1，后天=2；问「预报/未来」返回 None（发多天）。"""
        if "大后天" in text:
            return 3
        if "后天" in text:
            return 2
        if "明天" in text:
            return 1
        if "今天" in text:
            return 0
        return None

    async def build_reply(self, city: str, raw_text: str) -> Union[str, List[str]]:
        """返回要发的内容：str = 一条消息；list = 每条一个消息（预报按天拆开）。"""
        day = self._day_index(raw_text)
        want_forecast = day is not None or any(k in raw_text for k in ("预报", "未来", "这周", "本周"))
        data, err = await self.fetch_weather(city or self.default_city, want_forecast)
        if not data:
            return f"🌧 {err}"
        if want_forecast:
            # 问「未来/预报」时不指定具体某天 → 直接给 4 天
            if any(k in raw_text for k in ("预报", "未来", "这周", "本周")) and day in (0, None):
                return self.format_forecast(data, None)
            return self.format_forecast(data, day)
        return self.format_now(data)

    def _should_reply(self, event: AstrMessageEvent) -> bool:
        if self.only_when_woken and not getattr(event, "is_at_or_wake_command", True):
            return False
        return True

    async def _send_reply(self, event: AstrMessageEvent, parts: Union[str, List[str]]) -> None:
        """把 build_reply 的结果发出去。

        预报是 list 时，**一条一条自己发**：不依赖全局「分段回复」开关，
        也不把多段塞进同一个消息链（那样 QQ 会把几段文字挤成一条、连换行都没了）。
        发完 stop_event()，避免事件继续往下走又被大模型补一句。
        """
        texts = [parts] if isinstance(parts, str) else [p for p in parts if p and p.strip()]
        if not texts:
            texts = ["没拿到天气数据，稍后再试试。"]

        for i, t in enumerate(texts):
            if i:
                await asyncio.sleep(SPLIT_DELAY)  # 两条之间留点间隔，读起来更自然
            try:
                await event.send(MessageChain([Plain(t)]))
            except Exception as e:  # noqa: BLE001 - 发送失败别把插件搞崩
                logger.error("[天气] 第 %s 条消息发送失败：%s", i + 1, e)
                break

        event.stop_event()

    # ==================================================================
    # 六、事件入口（正则匹配，不受唤醒前缀限制）
    # ==================================================================

    @filter.regex(SUFFIX_PATTERN)
    async def on_weather_suffix(self, event: AstrMessageEvent):
        """温州瑞安天气 / 上海天气怎么样 / 北京天气预报"""
        text = event.get_message_str().strip()
        m = SUFFIX_PATTERN.search(text)
        if not m:
            return
        if not self._should_reply(event):
            return
        city = self._clean_city(m.group("city")) or self.default_city
        logger.info("[天气] 后缀式触发: %s → %s", text, city)
        await self._send_reply(event, await self.build_reply(city, text))

    @filter.regex(PREFIX_PATTERN)
    async def on_weather_prefix(self, event: AstrMessageEvent):
        """天气 上海 / 天气 温州瑞安"""
        text = event.get_message_str().strip()
        m = PREFIX_PATTERN.search(text)
        if not m:
            return
        if not self._should_reply(event):
            return
        city = self._clean_city(m.group("city")) or self.default_city
        logger.info("[天气] 前缀式触发: %s → %s", text, city)
        await self._send_reply(event, await self.build_reply(city, text))

    @filter.regex(EXACT_PATTERN)
    async def on_weather_default(self, event: AstrMessageEvent):
        """只发「天气」两个字 → 查默认城市"""
        text = event.get_message_str().strip()
        if not EXACT_PATTERN.search(text) or PREFIX_PATTERN.search(text):
            return
        if not self._should_reply(event):
            return
        logger.info("[天气] 默认城市触发: %s", text)
        await self._send_reply(event, await self.build_reply(self.default_city, text))
