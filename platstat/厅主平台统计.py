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
gameHallCategory=miscellaneous.gameHallName

def total_bet_score(hallName, sdate, edate):
    '''
    厅主平台首页面的厅主总投注额
    :param hallName: 厅主名称
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回厅主该月投注额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_name": hallName, "is_cancel": {"$eq": 0}}},
            {"$group": {"_id": "null", "总投注": {"$sum": "$total_bet_score"}}}
        ]
    )
    co = list(total).copy()
    if bool(co):
        return list(co)[0]["总投注"]
    else:
        return 0


def operator_win_score(hallName, sdate, edate):
    '''
    厅主平台首页面的厅主总盈利额
    :param hallName: 厅主名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回厅主该月盈利额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_name": hallName, "is_cancel": {"$eq": 0}}},
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


# 厅主的今日注单数
betcount = lambda hallID, starttime, endtime: db.get_collection("user_order").find(
    {"add_time": {"$gte": starttime,
                  "$lte": endtime}, "status": 4, "is_cancel": {"$ne": 1},
     "hall_id": hallID}).count()


def hallProfitTop10(hallName, sdate, edate):
    '''
    厅主平台代理盈利排行榜
    :param hallName: 厅主名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回厅主下的代理的赢钱排行
    '''
    hallprofit = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "end_time": {"$gte": sdate,
                             "$lte": edate}, "is_cancel": {"$eq": 0},
                "hall_name": hallName
            }
        }, {"$project": {"agent_name": 1, "operator_win_score": 1}},
        {"$group": {"_id": {"代理编号": "$agent_id", "代理名称": "$agent_name"}, "合计": {"$sum": "$operator_win_score"}}},
        {"$sort": {"合计": -1}},
        {"$limit": 10}
    ])
    co = list(hallprofit).copy()
    li = dict()
    if bool(co):
        for i in co:
            li.update({i['_id']['代理名称']: i['合计']})
        return li
    else:
        return {'None': 0}


def hallActiveMemberTop10(hallName, sdate, edate):
    '''
    厅主平台活跃会员数名
    :param hallName:厅主名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 厅主所属代理下的活跃会员数
    '''
    acitvemember = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_name": hallName,
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"代理编号": "$agent_id", "代理名称": "$agent_name"}, "会员首次下注": {"$addToSet": "$user_id"}}},
        {"$limit": 10}
    ])
    co = list(acitvemember).copy()
    li = dict()
    if bool(co):
        for i in co:
            li.update({i['_id']['代理名称']: len(i['会员首次下注'])})
        return sorted(li.items(), key=operator.itemgetter(1), reverse=True)
    else:
        return {'None': 0}


def queryReport(hallName, sdate, edate):
    '''
    厅主平台查询总报表
    :param hallName: 厅主名称
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回该厅主下各代理的投注和盈利情况
    '''
    agentReport = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_name": hallName,
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$project": {"agent_id": 0}},
        {"$group": {"_id": {"代理编号": "$agent_id", "代理名称": "$agent_name"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "代理盈利": {"$sum": "$operator_win_score"}}}
    ])
    co = list(agentReport).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'代理名称': hallName}, '笔数': 0, '有效投注': 0, '总下注金额': 0, '代理盈利': 0}]


def spicalPlayerReport(playerName, sdate, edate):
    '''
    厅主平台查询指定玩家
    :param playerName: 含有前缀的玩家名称
    :param sdate:开始时间
    :param edate:结束时间
    :return:返回该厅主下指定玩家的投注和盈利情况
    '''
    playerReport = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "account": playerName,
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"玩家名": "$account", "游戏厅": "$game_hall_id", "游戏": "$game_name"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "玩家盈利": {"$sum": "$total_win_score"}}}
    ])
    co = list(playerReport).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'玩家名': playerName, '游戏厅': 0, '游戏': '龙虎'}, '笔数': 0, '有效投注': 0, '总下注金额': 0, '玩家盈利': 0}]


def hallGameReport(hallName, sdate, edate):
    '''
    厅主平台查询指定游戏
    :param hallName:厅主名称
    :param sdate:开始时间
    :param edate:结束时间
    :return:返回该厅主下的所有游戏的投注及盈利情况
    '''
    gameReport = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_name": hallName,
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"厅主名": "$hall_name", "游戏": "$game_name"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "厅主盈利": {"$sum": "$operator_win_score"}}}
    ])
    co = list(gameReport).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'厅主名': hallName, '游戏': 'None'}, '笔数': 0, '有效投注': 0, '总下注金额': 0, '厅主盈利': 0}]


def spicalAgentReport(agentName, sdate, edate):
    '''
    厅主平台查询指定代理
    :param agentName: 含有前缀的代理名称
    :param sdate:开始时间
    :param edate:结束时间
    :return:返回该厅主下指定代理的投注和盈利情况
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
        {"$group": {"_id": {"代理名": "$agent_name", "游戏厅": "$game_hall_id", "游戏": "$game_name"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "代理盈利": {"$sum": "$operator_win_score"}}}
    ])
    co = list(agentReport).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'代理名': agentName, '游戏厅': 0, '游戏': '龙虎'}, '笔数': 0, '有效投注': 0, '总下注金额': 0, '玩家盈利': 0}]


@xw.func(async_mode='threading')
def hallFirstPage():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    hallName = wbc.sheets["厅主平台"].range("F2").value
    hallID = wbc.sheets["厅主平台"].range("F3").value
    wb.sheets["厅主平台"].range("B1").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["厅主平台"].range("C3").value = operator_win_score(hallName, startMonth, endMonth)
    wb.sheets["厅主平台"].range("C4").value = total_bet_score(hallName, startMonth, endMonth)
    wb.sheets["厅主平台"].range("C7").value = betcount(hallID, starttime, endtime)
    wb.sheets["厅主平台"].range("C9").value = operator_win_score(hallName, starttime, endtime)
    wb.sheets["厅主平台"].range("C10").value = total_bet_score(hallName, starttime, endtime)
    wb.sheets["厅主平台"].range("A14").options(transpose=True).value = list(
        range(1, len(hallProfitTop10(hallName, starttime, endtime)) + 1))
    wb.sheets["厅主平台"].range("B14").options(transpose=True).value = list(
        hallProfitTop10(hallName, starttime, endtime).keys())
    wb.sheets["厅主平台"].range("C14").options(transpose=True).value = list(
        hallProfitTop10(hallName, starttime, endtime).values())
    for k, v in enumerate(hallActiveMemberTop10(hallName, starttime, endtime), 1):
        wb.sheets["厅主平台"].range("A" + str(26 + k)).value = k
        wb.sheets["厅主平台"].range("B" + str(26 + k)).value = v[0]
        wb.sheets["厅主平台"].range("C" + str(26 + k)).value = v[1]


@xw.func(async_mode='threading')
def hallReportStat():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    hallName = wbc.sheets["厅主平台"].range("F2").value
    wb.sheets["厅主报表统计"].range("B1").value = datetime.utcnow() - timedelta(hours=4)
    for i, v in enumerate(queryReport(hallName, starttime, endtime), 1):
        wb.sheets["厅主报表统计"].range("A" + str(4 + i)).value = [v['_id']['代理名称'], v['笔数'], v['有效投注'], v['总下注金额'],
                                                             v['代理盈利']]


@xw.func(async_mode='threading')
def PlayerReport():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    playerName = wbc.sheets["厅主报表统计"].range("H36").value
    for i, v in enumerate(spicalPlayerReport(playerName, starttime, endtime), 1):
        wb.sheets["厅主报表统计"].range("A" + str(39 + i)).value = [v['_id']['玩家名'], gameHallCategory(v['_id']['游戏厅']), v['_id']['游戏'], v['笔数'],
                                                              v['有效投注'], v['总下注金额'], v['玩家盈利']]


@xw.func(async_mode='threading')
def specialHallGameReport():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    hallName = wbc.sheets["厅主平台"].range("F2").value
    for i, v in enumerate(hallGameReport(hallName, starttime, endtime), 1):
        wb.sheets["厅主报表统计"].range("A" + str(64 + i)).value = [v['_id']['厅主名'], v['_id']['游戏'], v['笔数'], v['有效投注'],
                                                              v['总下注金额'], v['厅主盈利']]


@xw.func(async_mode='threading')
def specialAgentReport():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    agentName = wbc.sheets["厅主报表统计"].range("H89").value
    for i, v in enumerate(spicalAgentReport(agentName, starttime, endtime), 1):
        wb.sheets["厅主报表统计"].range("A" + str(92 + i)).value = [v['_id']['代理名'], gameHallCategory(v['_id']['游戏厅']), v['_id']['游戏'], v['笔数'],
                                                              v['有效投注'], v['总下注金额'], v['代理盈利']]