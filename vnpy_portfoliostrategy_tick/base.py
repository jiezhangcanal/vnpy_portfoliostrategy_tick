from enum import Enum
from vnpy.trader.constant import Interval

APP_NAME = "PortfolioStrategy"


class EngineType(Enum):
    LIVE = "实盘"
    BACKTESTING = "回测"

class BacktestingMode(Enum):
    BAR = 1
    TICK = 2

class LimitType(Enum):
    LAST = "1"
    BIDASK1 = "2"
    BIDASK2 = "3"
    BIDASK3 = "4"
    BIDASK4 = "5"
    BIDASK5 = "6"

EVENT_PORTFOLIO_LOG = "ePortfolioLog"
EVENT_PORTFOLIO_STRATEGY = "ePortfolioStrategy"


members = []
for name, member in Interval.__members__.items():
    members.append((name, member.value))
members.append(("SECOND", '1s'))
Interval =Enum("Interval", members)