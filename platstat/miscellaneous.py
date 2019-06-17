from pymongo import MongoClient
from datetime import datetime, timedelta
import os
# 获取美东时间
sESTtime = datetime.utctimetuple(datetime.utcnow() - timedelta(hours=4))
eESTtime = datetime.utctimetuple(datetime.utcnow() + timedelta(hours=20))
#一天时间
starttime = datetime(sESTtime.tm_year, sESTtime.tm_mon, sESTtime.tm_mday, 4, 00, 00, 00)
endtime = datetime(eESTtime.tm_year, eESTtime.tm_mon, eESTtime.tm_mday, 3, 59, 59)
# 一个月时间
startMonth = datetime(sESTtime.tm_year, sESTtime.tm_mon, 1, 4, 00, 00, 00)
endMonth = datetime(sESTtime.tm_year, sESTtime.tm_mon + 1, 1, 3, 59, 59, 00)
sdayofmonth=[startMonth+timedelta(days=i) for i in range(0,32) if startMonth+timedelta(days=i)<=endMonth]
edayofmonth=[datetime(eESTtime.tm_year, eESTtime.tm_mon, 1+i, 3, 59, 59) for i in range(1,32) if startMonth+timedelta(days=i)<=endMonth]
edayofmonth.append(endMonth)
#一月中每一天的数据
daysOfMonth=zip(sdayofmonth,edayofmonth)
#联调厅主和测试厅主ID
excludehalllist = [379, 384, 390, 393, 398, 403, 404, 405, 418, 426, 427, 431, 433, 434, 436, 440, 442, 445, 446, 451,
                   452, 456, 466, 502, 504, 533, 534, 537, 538, 541, 546, 547, 580, 581, 582, 583, 608, 621, 622, 632,
                   634, 638, 664, 665, 670, 674, 695, 696, 762, 844, 845, 846, 847, 848, 849, 850, 854, 855, 858, 1387,
                   1412, 1413, 1415, 1462, 1498, 1516, 1525, 1526, 1543, 1560, 1572, 1609, 1610, 1636, 1644, 1645, 1646,
                   1647, 1705, 1706, 1708, 1709, 1711, 1713, 1714, 1722, 1723, 1809, 1813, 1818, 1819, 1823, 1826, 1829,
                   1843, 1846, 1850, 1]
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
executeFile = os.path.join(BASE_DIR, '平台数据核对.xlsm')
#mongodb登录认证
def mongo_auth():
    client = MongoClient("10.200.124.27", 30000)
    db = client.get_database("live_game")
    db.authenticate("Liveadmin", "Lv201888*")
    return db

#0:旗舰厅 1:贵宾厅
#2:金臂厅 3:至尊厅
def gameHallName(num):
    if num==0:
        return '旗舰厅'
    elif num==1:
        return '贵宾厅'
    elif num==2:
        return '金臂厅'
    elif num==3:
        return '至尊厅'
    else:
        return '钻石厅'


