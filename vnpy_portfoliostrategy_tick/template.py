from abc import ABC
from copy import copy
from typing import Dict, Set, List, TYPE_CHECKING, Optional
from collections import defaultdict

from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual

from .base import EngineType, BacktestingMode, LimitType

if TYPE_CHECKING:
    from .engine import StrategyEngine


class StrategyTemplate(ABC):
    """组合策略模板"""

    author: str = ""
    parameters: list = []
    variables: list = []
    settings: list = ['limit', 'mode', 'limit_type']

    def __init__(
        self,
        strategy_engine: "StrategyEngine",
        strategy_name: str,
        vt_symbols: List[str],
        setting: dict
    ) -> None:
        """构造函数"""
        self.strategy_engine: "StrategyEngine" = strategy_engine
        self.strategy_name: str = strategy_name
        self.vt_symbols: List[str] = vt_symbols

        # 状态控制变量
        self.inited: bool = False
        self.trading: bool = False
        self.limit: bool = True
        self.limit_type : LimitType = LimitType.LAST
        self.mode: BacktestingMode = BacktestingMode.BAR
        self.close_price_tick = {}
        self.bid_price_1_tick = {}
        self.ask_price_1_tick = {}
        self.bid_price_2_tick = {}
        self.ask_price_2_tick = {}
        self.bid_price_3_tick = {}
        self.ask_price_3_tick = {}
        self.bid_price_4_tick = {}
        self.ask_price_4_tick = {}
        self.bid_price_5_tick = {}
        self.ask_price_5_tick = {}

        # 持仓数据字典
        self.pos_data: Dict[str, int] = defaultdict(int)        # 实际持仓
        self.target_data: Dict[str, int] = defaultdict(int)     # 目标持仓

        # 委托缓存容器
        self.orders: Dict[str, OrderData] = {}
        self.active_orderids: Set[str] = set()

        # 复制变量名列表，插入默认变量内容
        self.variables: list = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "pos_data")
        self.variables.insert(3, "target_data")

        # 设置策略参数
        self.update_setting(setting)

    def update_setting(self, setting: dict) -> None:
        """设置策略参数"""
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])
        for name in self.settings:
            if name in setting:
                setattr(self, name, setting[name])

    @classmethod
    def get_class_parameters(cls) -> dict:
        """查取策略默认参数"""
        class_parameters: dict = {}
        for name in cls.parameters:
            class_parameters[name] = getattr(cls, name)
        return class_parameters

    def get_parameters(self) -> dict:
        """查询策略参数"""
        strategy_parameters: dict = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self) -> dict:
        """查询策略变量"""
        strategy_variables: dict = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self) -> dict:
        """查询策略状态数据"""
        strategy_data: dict = {
            "strategy_name": self.strategy_name,
            "vt_symbols": self.vt_symbols,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    @virtual
    def on_init(self) -> None:
        """策略初始化回调"""
        pass

    @virtual
    def on_start(self) -> None:
        """策略启动回调"""
        pass

    @virtual
    def on_stop(self) -> None:
        """策略停止回调"""
        pass

    @virtual
    def on_tick(self, tick: TickData) -> None:
        """行情推送回调"""
        pass
    
    @virtual
    def on_tick_9999(self, tick: TickData) -> None:
        """行情推送回调"""
        self.close_price_tick[tick.vt_symbol] = tick.last_price
        self.bid_price_1_tick[tick.vt_symbol] = tick.bid_price_1
        self.ask_price_1_tick[tick.vt_symbol] = tick.ask_price_1
        self.bid_price_2_tick[tick.vt_symbol] = tick.bid_price_2
        self.ask_price_2_tick[tick.vt_symbol] = tick.ask_price_2
        self.bid_price_3_tick[tick.vt_symbol] = tick.bid_price_3
        self.ask_price_3_tick[tick.vt_symbol] = tick.ask_price_3
        self.bid_price_4_tick[tick.vt_symbol] = tick.bid_price_4
        self.ask_price_4_tick[tick.vt_symbol] = tick.ask_price_4
        self.bid_price_5_tick[tick.vt_symbol] = tick.bid_price_5
        self.ask_price_5_tick[tick.vt_symbol] = tick.ask_price_5


    @virtual
    def on_bars(self, bars: Dict[str, BarData]) -> None:
        """K线切片回调"""
        pass

    def update_trade(self, trade: TradeData) -> None:
        """成交数据更新"""
        if trade.direction == Direction.LONG:
            self.pos_data[trade.vt_symbol] += trade.volume
        else:
            self.pos_data[trade.vt_symbol] -= trade.volume

    def update_order(self, order: OrderData) -> None:
        """委托数据更新"""
        self.orders[order.vt_orderid] = order

        if not order.is_active() and order.vt_orderid in self.active_orderids:
            self.active_orderids.remove(order.vt_orderid)

    def buy(self, vt_symbol: str, price: float, volume: float, lock: bool = False, net: bool = False) -> List[str]:
        """买入开仓"""
        return self.send_order(vt_symbol, Direction.LONG, Offset.OPEN, price, volume, lock, net)

    def sell(self, vt_symbol: str, price: float, volume: float, lock: bool = False, net: bool = False) -> List[str]:
        """卖出平仓"""
        return self.send_order(vt_symbol, Direction.SHORT, Offset.CLOSE, price, volume, lock, net)

    def short(self, vt_symbol: str, price: float, volume: float, lock: bool = False, net: bool = False) -> List[str]:
        """卖出开仓"""
        return self.send_order(vt_symbol, Direction.SHORT, Offset.OPEN, price, volume, lock, net)

    def cover(self, vt_symbol: str, price: float, volume: float, lock: bool = False, net: bool = False) -> List[str]:
        """买入平仓"""
        return self.send_order(vt_symbol, Direction.LONG, Offset.CLOSE, price, volume, lock, net)

    def send_order(
        self,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> List[str]:
        """发送委托"""
        if self.trading:
            vt_orderids: list = self.strategy_engine.send_order(
                self, vt_symbol, direction, offset, price, volume, lock, net, self.limit
            )

            for vt_orderid in vt_orderids:
                self.active_orderids.add(vt_orderid)

            return vt_orderids
        else:
            return []

    def cancel_order(self, vt_orderid: str) -> None:
        """撤销委托"""
        if self.trading:
            self.strategy_engine.cancel_order(self, vt_orderid)

    def cancel_all(self) -> None:
        """全撤活动委托"""
        for vt_orderid in list(self.active_orderids):
            self.cancel_order(vt_orderid)

    def get_pos(self, vt_symbol: str) -> int:
        """查询当前持仓"""
        return self.pos_data.get(vt_symbol, 0)

    def get_target(self, vt_symbol: str) -> int:
        """查询目标仓位"""
        return self.target_data[vt_symbol]

    def set_target(self, vt_symbol: str, target: int) -> None:
        """设置目标仓位"""
        self.target_data[vt_symbol] = target

    def rebalance_portfolio(self, bars: Dict[str, BarData]) -> None:
        """基于目标执行调仓交易"""
        self.cancel_all()

        # 只发出当前K线切片有行情的合约的委托
        for vt_symbol, bar in bars.items():
            # 计算仓差
            target: int = self.get_target(vt_symbol)
            pos: int = self.get_pos(vt_symbol)
            diff: int = target - pos

            # 多头
            if diff > 0:
                # 计算多头委托价
                order_price: float = self.calculate_price(
                    vt_symbol,
                    Direction.LONG,
                    bar.close_price
                )

                # 计算买平和买开数量
                cover_volume: int = 0
                buy_volume: int = 0

                if pos < 0:
                    cover_volume = min(diff, abs(pos))
                    buy_volume = diff - cover_volume
                else:
                    buy_volume = diff

                # 发出对应委托
                if cover_volume:
                    self.cover(vt_symbol, order_price, cover_volume)

                if buy_volume:
                    self.buy(vt_symbol, order_price, buy_volume)
            # 空头
            elif diff < 0:
                # 计算空头委托价
                order_price: float = self.calculate_price(
                    vt_symbol,
                    Direction.SHORT,
                    bar.close_price
                )

                # 计算卖平和卖开数量
                sell_volume: int = 0
                short_volume: int = 0

                if pos > 0:
                    sell_volume = min(abs(diff), pos)
                    short_volume = abs(diff) - sell_volume
                else:
                    short_volume = abs(diff)

                # 发出对应委托
                if sell_volume:
                    self.sell(vt_symbol, order_price, sell_volume)

                if short_volume:
                    self.short(vt_symbol, order_price, short_volume)

    @virtual
    def calculate_price(
        self,
        vt_symbol: str,
        direction: Direction,
        reference: float
    ) -> float:
        """计算调仓委托价格（支持按需重载实现）"""
        print(f"calculate_price:{self.mode}")
        if self.mode == BacktestingMode.BAR:
            return reference
        else:
            vt_symbol = vt_symbol.replace("8888", "9999")
            if self.limit_type == LimitType.LAST:
                return self.close_price_tick[vt_symbol]
            else:
                if self.limit_type == LimitType.BIDASK1:
                    if direction == Direction.LONG:
                        return self.bid_price_1_tick[vt_symbol]
                    else:
                        return self.ask_price_1_tick[vt_symbol]
                elif self.limit_type == LimitType.BIDASK2:
                    if direction == Direction.LONG:
                        return self.get_bid_price_tick(vt_symbol, 2)
                    else:
                        return self.get_ask_price_tick(vt_symbol, 2)
                elif self.limit_type == LimitType.BIDASK3:
                    if direction == Direction.LONG:
                        return self.get_bid_price_tick(vt_symbol, 3)
                    else:
                        return self.get_ask_price_tick(vt_symbol, 3)
                elif self.limit_type == LimitType.BIDASK4:
                    if direction == Direction.LONG:
                        return self.get_bid_price_tick(vt_symbol, 4)
                    else:
                        return self.get_ask_price_tick(vt_symbol, 4)
                elif self.limit_type == LimitType.BIDASK5:
                    if direction == Direction.LONG:
                        return self.get_bid_price_tick(vt_symbol, 5)
                    else:
                        return self.get_ask_price_tick(vt_symbol, 5)
    
    def get_bid_price_tick(self, vt_symbol: str, level: int) -> float:
        price = self.get_original_bid_ask_price_tick(True, vt_symbol, level)
        if price is not None and price > 0:
            return price
        step = max(self.get_pricetick(vt_symbol.replace("9999", "8888")), (self.ask_price_1_tick[vt_symbol] - self.bid_price_1_tick[vt_symbol])/2)
        step_count = 1
        level -= 1
        while level > 1:
            price = self.get_original_bid_ask_price_tick(True, vt_symbol, level)
            if price is not None and price > 0:
                level -= 1
                step_count += 1
            else:
                break
        return self.get_original_bid_ask_price_tick(True, vt_symbol, level) - step * step_count

    def get_ask_price_tick(self, vt_symbol: str, level: int) -> float:
        price = self.get_original_bid_ask_price_tick(False, vt_symbol, level)
        if price is not None and price > 0:
            return price
        step = max(self.get_pricetick(vt_symbol.replace("9999", "8888")), (self.ask_price_1_tick[vt_symbol] - self.bid_price_1_tick[vt_symbol])/2)
        step_count = 1
        level -= 1
        while level > 1:
            price = self.get_original_bid_ask_price_tick(False, vt_symbol, level)
            if price is not None and price > 0:
                level -= 1
                step_count += 1
            else:
                break
        return self.get_original_bid_ask_price_tick(False, vt_symbol, level) + step * step_count

    
    def get_original_bid_ask_price_tick(self, bid, vt_symbol: str, level: int) -> float:
        if bid:
            if level == 1:
                return self.bid_price_1_tick[vt_symbol]
            elif level == 2:
                return self.bid_price_2_tick[vt_symbol]
            elif level == 3:
                return self.bid_price_3_tick[vt_symbol]
            elif level == 4:
                return self.bid_price_4_tick[vt_symbol]
            elif level == 5:
                return self.bid_price_5_tick[vt_symbol]
        else:
            if level == 1:
                return self.ask_price_1_tick[vt_symbol]
            elif level == 2:
                return self.ask_price_2_tick[vt_symbol]
            elif level == 3:
                return self.ask_price_3_tick[vt_symbol]
            elif level == 4:
                return self.ask_price_4_tick[vt_symbol]
            elif level == 5:
                return self.ask_price_5_tick[vt_symbol]
        return 0

    def get_order(self, vt_orderid: str) -> Optional[OrderData]:
        """查询委托数据"""
        return self.orders.get(vt_orderid, None)

    def get_all_active_orderids(self) -> List[OrderData]:
        """获取全部活动状态的委托号"""
        return list(self.active_orderids)

    def write_log(self, msg: str) -> None:
        """记录日志"""
        self.strategy_engine.write_log(msg, self)

    def get_engine_type(self) -> EngineType:
        """查询引擎类型"""
        return self.strategy_engine.get_engine_type()

    def get_pricetick(self, vt_symbol: str) -> float:
        """查询合约最小价格跳动"""
        return self.strategy_engine.get_pricetick(self, vt_symbol)

    def get_size(self, vt_symbol: str) -> int:
        """查询合约乘数"""
        return self.strategy_engine.get_size(self, vt_symbol)

    def load_bars(self, days: int, interval: Interval = Interval.MINUTE) -> None:
        """加载历史K线数据来执行初始化"""
        self.strategy_engine.load_bars(self, days, interval)

    def put_event(self) -> None:
        """推送策略数据更新事件"""
        if self.inited:
            self.strategy_engine.put_strategy_event(self)

    def send_email(self, msg: str) -> None:
        """发送邮件信息"""
        if self.inited:
            self.strategy_engine.send_email(msg, self)

    def sync_data(self) -> None:
        """同步策略状态数据到文件"""
        if self.trading:
            self.strategy_engine.sync_strategy_data(self)
