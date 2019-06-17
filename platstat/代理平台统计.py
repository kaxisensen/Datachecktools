import xlwings as xw
from datetime import datetime, timedelta
from platstat import miscellaneous
import operator

db = miscellaneous.mongo_auth()
starttime = miscellaneous.starttime
endtime = miscellaneous.endtime
startMonth = miscellaneous.startMonth
endMonth = miscellaneous.endMonth
excludehalllist = miscellaneous.excludehalllist
daysOfMonth = miscellaneous.daysOfMonth
executeFile = miscellaneous.executeFile


def total_bet_score(agentName, sdate, edate):
    '''
    代理平台首页面的总投注额
    :param agentName: 代理名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回当月指定代理的总投注额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "agent_name": agentName, "is_cancel": {"$eq": 0}}},
            {"$group": {"_id": "null", "总投注": {"$sum": "$total_bet_score"}}}
        ]
    )
    co = list(total).copy()
    if bool(co):
        return list(co)[0]["总投注"]
    else:
        return 0


def operator_win_score(agentName, sdate, edate):
    '''
    代理平台首页面的总派彩额
    :param agentName: 代理名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回当月指定代理的总派彩额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "agent_name": agentName, "is_cancel": {"$eq": 0}}},
            {"$group": {"_id": "null", "派彩金额": {"$sum": "$operator_win_score"}}}
        ]
    )
    co = list(total).copy()
    # 站在玩家的角度负数就是输钱
    if bool(co):
        winmoney = list(co)[0]["派彩金额"]
        return -winmoney if winmoney > 0 else abs(winmoney)
    else:
        return 0


# 今日注单数
betcount = lambda agentName, sdate, edate: db.get_collection("user_order").find(
    {"add_time": {"$gte": sdate,
                  "$lte": edate}, "status": 4, "is_cancel": {"$ne": 1},
     "agent_id": agentName}).count()


def agentProfitTop10(agentName, sdate, edate):
    '''
    代理平台首页面的会员盈利排行榜
    :param agentName: 代理名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 该代理下的赢钱最多的前10名玩家
    '''
    agentprofit = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "end_time": {"$gte": sdate,
                             "$lte": edate}, "is_cancel": {"$eq": 0},
                "agent_name": agentName
            }
        },
        {"$group": {"_id": {"玩家编号": "$user_id", "玩家名称": "$user_name"}, "合计": {"$sum": "$total_win_score"}}},
        {"$sort": {"合计": -1}},
        {"$limit": 10}
    ])
    co = list(agentprofit).copy()
    li = dict()
    if bool(co):
        for i in co:
            li.update({i['_id']['玩家名称']: i['合计']})
        return li
    else:
        return {'None': 0}


def agentActiveMemberTop10(agentID, sdate, edate):
    '''
    代理平台活跃会员数排名(按下注区域来统计的)
    :param agentID: 代理编号
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回该代理下的下注最多的前10名会员
    '''
    acitvemember = db.get_collection("user_order").aggregate([
        {
            "$match": {
                "agent_id": agentID,
                "add_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"玩家编号": "$user_id", "玩家名称": "$user_name"}, "会员下注": {"$sum": 1}}},
        {"$limit": 10}
    ])
    co = list(acitvemember).copy()
    li = dict()
    if bool(co):
        for i in co:
            li.update({i['_id']['玩家名称']: i['会员下注']})
        return sorted(li.items(), key=operator.itemgetter(1), reverse=True)
    else:
        return [('None', 0)]


def queryReport(agentName, sdate, edate):
    '''
    代理平台查询总报表
    :param agentName:代理名称
    :param sdate:开始时间
    :param edate:结束时间
    :return:返回该代理的下注及盈利情况
    '''
    agentReport = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "agent_name": agentName,
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$project": {"agent_id": 0}},
        {"$group": {"_id": {"玩家编号": "$user_id", "玩家名称": "$account"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "会员盈利": {"$sum": "$total_win_score"}}}
    ])
    co = list(agentReport).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'玩家编号': "None", '玩家名称': 'None'}, '笔数': 0, '有效投注': 0, '总下注金额': 0, '会员盈利': 0}]


def agentGameReport(agentName, sdate, edate):
    '''
    代理平台查询指定游戏
    :param agentName: 代理名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回该代理在所有游戏中的投注及盈利情况
    '''
    gameReport = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "agent_name": agentName,
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"代理名": "$agent_name", "游戏": "$game_name"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "代理盈利": {"$sum": "$operator_win_score"}}}
    ])
    co = list(gameReport).copy()
    if bool(co):
        return co
    else:
        return 0


@xw.func(async_mode='threading')
def agentFirstPage():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    agentName = wbc.sheets["代理平台"].range("F2").value
    agentID = wbc.sheets["代理平台"].range("F3").value
    wb.sheets["代理平台"].range("B1").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["代理平台"].range("C3").value = operator_win_score(agentName, startMonth, endMonth)
    wb.sheets["代理平台"].range("C4").value = total_bet_score(agentName, startMonth, endMonth)
    wb.sheets["代理平台"].range("C7").value = betcount(agentID, starttime, endtime)
    wb.sheets["代理平台"].range("C9").value = operator_win_score(agentName, starttime, endtime)
    wb.sheets["代理平台"].range("C10").value = total_bet_score(agentName, starttime, endtime)
    wb.sheets["代理平台"].range("A14").options(transpose=True).value = list(
        range(1, len(agentProfitTop10(agentName, starttime, endtime)) + 1))
    wb.sheets["代理平台"].range("B14").options(transpose=True).value = list(
        agentProfitTop10(agentName, starttime, endtime).keys())
    wb.sheets["代理平台"].range("C14").options(transpose=True).value = list(
        agentProfitTop10(agentName, starttime, endtime).values())
    wb.sheets["代理平台"].range("A27").options(transpose=True).value = list(
        range(1, len(agentActiveMemberTop10(agentID, starttime, endtime)) + 1))
    wb.sheets["代理平台"].range("B27").options(transpose=True).value = list(
        dict(agentActiveMemberTop10(agentID, starttime, endtime)).keys())
    wb.sheets["代理平台"].range("C27").options(transpose=True).value = list(
        dict(agentActiveMemberTop10(agentID, starttime, endtime)).values())


@xw.func(async_mode='threading')
def agentGameStat():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    agentName = wbc.sheets["代理平台"].range("F2").value
    agentID = wbc.sheets["代理平台"].range("F3").value
    for i, v in enumerate(agentGameReport(agentName, starttime, endtime), 1):
        wb.sheets["代理平台"].range("A" + str(40 + i)).value = [v['_id']['代理名'], v['_id']['游戏'], v['笔数'], v['有效投注'],
                                                            v['总下注金额'],
                                                            v['代理盈利']]


@xw.func(async_mode='threading')
def playerGameReport():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    agentName = wbc.sheets["代理平台"].range("F2").value
    for i, v in enumerate(queryReport(agentName, starttime, endtime), 1):
        wb.sheets["代理平台"].range("A" + str(65 + i)).value = [v['_id']['玩家名称'], v['笔数'], v['有效投注'],
                                                            v['总下注金额'], v['会员盈利']]
