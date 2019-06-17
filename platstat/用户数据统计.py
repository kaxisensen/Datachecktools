import xlwings as xw
from datetime import datetime, timedelta
from platstat import miscellaneous

db = miscellaneous.mongo_auth()
starttime = miscellaneous.starttime
endtime = miscellaneous.endtime
startMonth = miscellaneous.startMonth
endMonth = miscellaneous.endMonth
excludehalllist = miscellaneous.excludehalllist
daysOfMonth = miscellaneous.daysOfMonth


def pltStatByDays(sdate, edate):
    '''
    总平台用户数据统计的平台统计模块按天统计
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回平台统计模块按天统计
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [{
            "$match": {
                "end_time": {
                    "$gte": sdate,
                    "$lte": edate
                },
                "hall_id": {
                    "$nin": excludehalllist
                }, "is_cancel": {"$eq": 0}
            }
        },
            {"$group": {"_id": "null", "下注金额": {"$sum": "$total_bet_score"}, "派彩金额": {"$sum": "$operator_win_score"}}},
            {"$project": {"_id": 0, "下注金额": 1, "派彩金额": 1}}
        ]
    )
    return list(total)


def pltStatByMons(sMonth, eMonth):
    '''
    总平台用户数据统计的平台统计模块按月统计
    :param sMonth: 开始月份
    :param eMonth: 结束月份
    :return: 返回平台统计模块按月统计
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [{
            "$match": {
                "end_time": {
                    "$gte": sMonth,
                    "$lte": eMonth
                },
                "hall_id": {
                    "$nin": excludehalllist
                }, "is_cancel": {"$eq": 0}
            }
        },
            {"$group": {"_id": "null", "下注金额": {"$sum": "$total_bet_score"}, "派彩金额": {"$sum": "$operator_win_score"}}},
            {"$project": {"_id": 0, "下注金额": 1, "派彩金额": 1}}
        ]
    )
    return list(total)


# 总平台用户数据统计的平台统计模块的今日注单数
betcount = lambda startdate, enddate: db.get_collection("user_order").find(
    {"add_time": {"$gte": startdate,
                  "$lte": enddate}, "status": 4, "is_cancel": {"$ne": 1},
     "hall_id": {
         "$nin": excludehalllist
     }}).count()

# 总平台用户数据统计的玩家统计仅统计每天每一次下注的玩家
activeMemberStat = lambda startdate, enddate: list(db.get_collection("user_chart_info").aggregate(
    [
        {"$match": {"end_time": {"$gte": startdate,
                                 "$lte": enddate},
                    "hall_id": {"$nin": excludehalllist}}},
        {"$group": {"_id": "null", "玩家": {"$addToSet": "$user_name"}}}
    ]))


def monthOfactiveMemberStat():
    '''
    平台用户数据统计的玩家统计之月统计
    :return: 返回总平台用户数据统计的玩家统计之月统计
    '''
    count = 0
    for sday, eday in daysOfMonth:
        if len(activeMemberStat(sday, eday)):
            count += len(activeMemberStat(sday, eday)[0]['玩家'])
    return count


def betTimeIntervalDistribution(sdate, edate):
    '''
    总平台用户数据统计之在线统计时段投注额分布
    :param sdate: 开始日期
    :param edate: 结束在
    :return: 返回在线统计时段投注额分布
    '''
    result = []
    moneyregion = [(1, 1000), (1000, 5000), (5000, 10000), (10000, 50000), (50000, 200000), (200000, 10000000)]
    for i in range(24):
        stime = sdate + timedelta(hours=i)
        etime = edate + timedelta(hours=i + 1)
        for j in moneyregion:
            moneyDistribution = db.get_collection("user_order").aggregate(
                [
                    {"$match": {"add_time": {"$gte": stime,
                                             "$lte": etime},
                                "hall_id": {"$nin": excludehalllist},
                                "bet_money": {"$gte": j[0], "$lt": j[1]}
                                }},
                    {"$group": {"_id": "null", "合计": {"$sum": "$bet_money"}}},
                    {"$project": {"_id": 0, "合计": 1}}
                ])
            # result.append([{"时间":str(i)+'-'+str(i+1)},{"区间":j},list(moneyDistribution)])
            result.append(list(moneyDistribution))
    return result


def betsIntervalDistribution(sdate, edate):
    '''
    投注额区间注单数
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回投注额区间注单数
    '''
    result = []
    moneyregion = [(1, 1000), (1000, 5000), (5000, 10000), (10000, 50000), (50000, 200000), (200000, 10000000)]
    # stime = starttime + timedelta(hours=i)
    # etime = starttime + timedelta(hours=i + 1)
    for j in moneyregion:
        betsDistribution = db.get_collection("user_order").aggregate(
            [
                {"$match": {"add_time": {"$gte": sdate,
                                         "$lte": edate},
                            "hall_id": {"$nin": excludehalllist},
                            "bet_money": {"$gte": j[0], "$lt": j[1]}
                            }},
                {"$group": {"_id": "null", "合计": {"$sum": 1}}},
                {"$project": {"_id": 0, "合计": 1}}
            ])
        result.append(list(betsDistribution))
    # 昨日注单数统计
    ysday = starttime - timedelta(days=1)
    yeday = endtime - timedelta(days=1)
    yresult = []
    for j in moneyregion:
        yesterdaybetsDistribution = db.get_collection("user_order").aggregate(
            [
                {"$match": {"add_time": {"$gte": ysday,
                                         "$lte": yeday},
                            "hall_id": {"$nin": excludehalllist},
                            "bet_money": {"$gte": j[0], "$lt": j[1]}
                            }},
                {"$group": {"_id": "null", "合计": {"$sum": 1}}},
                {"$project": {"_id": 0, "合计": 1}}
            ])
        yresult.append(list(yesterdaybetsDistribution))
    return (yresult, result)


@xw.func(async_mode='threading')
def platStat():
    """平台统计"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B1").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C2").value = pltStatByDays(starttime, endtime)[0]["派彩金额"]
    wb.sheets["用户数据统计"].range("C3").value = pltStatByDays(starttime, endtime)[0]["下注金额"]
    wb.sheets["用户数据统计"].range("B5").value = wb.sheets["用户数据统计"].range("B1").value
    wb.sheets["用户数据统计"].range("C6").value = pltStatByMons(startMonth, endMonth)[0]["派彩金额"]
    wb.sheets["用户数据统计"].range("C7").value = pltStatByMons(startMonth, endMonth)[0]["下注金额"]
    wb.sheets["用户数据统计"].range("B9").value = wb.sheets["用户数据统计"].range("B1").value
    wb.sheets["用户数据统计"].range("C10").value = betcount(starttime, endtime)
    wb.sheets["用户数据统计"].range("B12").value = wb.sheets["用户数据统计"].range("B1").value
    wb.sheets["用户数据统计"].range("C13").value = betcount(startMonth, endMonth)


@xw.func(async_mode='threading')
def playerOnlineStat():
    """总平台用户数据统计之玩家统计"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B19").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C21").value = len(activeMemberStat(starttime, endtime)[0]['玩家']) if len(
        activeMemberStat(starttime, endtime)) > 0 else 0
    wb.sheets["用户数据统计"].range("B23").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C25").value = monthOfactiveMemberStat()


@xw.func(async_mode='threading')
def onlineStat():
    """总平台用户数据统计之在线统计"""
    wb = xw.Book.caller()
    li = [chr(i) for i in range(ord("B"), ord("Z"))]
    betlist = betTimeIntervalDistribution(starttime, endtime)
    t = 0
    for i in li:
        cell = 29
        for j in range(6):
            wb.sheets["用户数据统计"].range(i + str(cell)).value = (0 if len(betlist[t]) == 0 else betlist[t][0]['合计'])
            cell += 1
            t += 1
    # 投注额区间注单数
    li = [chr(i) for i in range(ord("B"), ord("C") + 1)]
    zhudan = betsIntervalDistribution(starttime, endtime)
    for j, i in zip(li, zhudan):
        cell = 39
        for k in range(len(i)):
            wb.sheets["用户数据统计"].range(j + str(cell)).value = (0 if len(i[k]) == 0 else i[k][0]['合计'])
            cell += 1
