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


def gameStatByProfitdata(sdate, edate):
    '''
    总平台用户数据统计之游戏统计盈利数据
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回指定日期的游戏统计盈利数据
    '''
    gameCategoryStat = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist},
                        "is_cancel": {"$eq": 0}
                        }},
            {"$group": {"_id": "$game_name", "派彩金额": {"$sum": "$operator_win_score"}}}
        ])
    gStat = list(gameCategoryStat).copy()
    if list(gStat):
        playstat = dict()
        for i in list(gStat):
            playstat.update({i["_id"]: i['派彩金额']})
        return playstat
    else:
        return {'骰宝': 0, '轮盘': 0, '龙虎': 0, '极速百家乐': 0, '百家乐': 0, '龙虎百家乐': 0, '金臂厅百家乐': 0, '德州牛仔': 0, '赌场扑克': 0,
                '炸金花': 0, '百人牛牛': 0, '21点': 0, '龙虎百家乐【鑽石廳】': 0, '共咪': 0}


def gameStatByBetdata(sdate, edate):
    '''
    总平台用户数据统计之游戏统计注单数据
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回游戏统计注单数据
    '''
    gameCategoryStat = db.get_collection("user_order").aggregate(
        [
            {"$match": {"add_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist},
                        "status": 4, "is_cancel": {"$ne": 1}
                        }},
            {"$group": {"_id": "$game_name", "count": {"$sum": 1}}}
        ])
    gStat = list(gameCategoryStat).copy()
    if list(gStat):
        playstat = dict()
        for i in list(gStat):
            playstat.update({i["_id"]: i['count']})
        return playstat
    else:
        return {'骰宝': 0, '轮盘': 0, '龙虎': 0, '极速百家乐': 0, '百家乐': 0, '龙虎百家乐': 0, '金臂厅百家乐': 0, '德州牛仔': 0, '赌场扑克': 0,
                '炸金花': 0, '百人牛牛': 0, '21点': 0, '龙虎百家乐【鑽石廳】': 0, '共咪': 0}


def gameStatBytouzhudata(sdate, edate):
    '''
    总平台用户数据统计之游戏统计投注数据
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回游戏统计投注数据
    '''
    gametouzhuStat = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist},
                        "is_cancel": {"$eq": 0}
                        }},
            {"$group": {"_id": "$game_name", "count": {"$sum": "$valid_bet_score_total"}}}
        ])
    gStat = list(gametouzhuStat).copy()
    if list(gStat):
        playstat = dict()
        for i in list(gStat):
            playstat.update({i["_id"]: i['count']})
        return playstat
    else:
        return {'骰宝': 0, '轮盘': 0, '龙虎': 0, '极速百家乐': 0, '百家乐': 0, '龙虎百家乐': 0, '金臂厅百家乐': 0, '德州牛仔': 0, '赌场扑克': 0,
                '炸金花': 0, '百人牛牛': 0, '21点': 0, '龙虎百家乐【鑽石廳】': 0, '共咪': 0}


def gameStatByPlayerdata(sdate, edate):
    '''
    总平台用户数据统计之游戏统计玩家数据
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回游戏统计玩家数据
    '''
    game = ['龙虎百家乐', '金臂厅百家乐', '百家乐', '龙虎', '轮盘', '骰宝', '极速百家乐', '十三水', '德州牛仔', '赌场扑克', '炸金花', '百人牛牛', '21点', '色碟',
            '龙虎百家乐【鑽石廳】', '共咪']
    gameplayerStat = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist},
                        "is_cancel": {"$eq": 0}
                        }},
            {"$group": {"_id": {"game_name": "$game_name", "user_name": "$user_name"}, "count": {"$sum": 1}}}
        ])
    gStat = list(gameplayerStat).copy()
    coll = []
    d = dict()
    # 先追加到列表然后组装成字典
    if gStat:
        for i in gStat:
            coll.append(i['_id']['game_name'])
        for j in game:
            d.update({j: coll.count(j)})
        return d
    else:
        return 0


@xw.func(async_mode='threading')
def GameStatByDay():
    """游戏统计按天"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B93").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A94").options(transpose=True).value = list(
        gameStatByProfitdata(starttime, endtime).keys())
    wb.sheets["用户数据统计"].range("B94").options(transpose=True).value = list(
        gameStatByProfitdata(starttime, endtime).values())


@xw.func(async_mode='threading')
def GameStatByMonth():
    """游戏统计按月"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B113").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A114").options(transpose=True).value = list(
        gameStatByProfitdata(startMonth, endMonth).keys())
    wb.sheets["用户数据统计"].range("B114").options(transpose=True).value = list(
        gameStatByProfitdata(startMonth, endMonth).values())


@xw.func(async_mode='threading')
def GameBetStatByDay():
    """游戏注单数据统计按天"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B133").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A134").options(transpose=True).value = list(
        gameStatByBetdata(starttime, endtime).keys())
    wb.sheets["用户数据统计"].range("B134").options(transpose=True).value = list(
        gameStatByBetdata(starttime, endtime).values())


@xw.func(async_mode='threading')
def GameBetStatByMonth():
    """游戏注单数据统计按月"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B174").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A175").options(transpose=True).value = list(
        gameStatByBetdata(startMonth, endMonth).keys())
    wb.sheets["用户数据统计"].range("B175").options(transpose=True).value = list(
        gameStatByBetdata(startMonth, endMonth).values())


@xw.func(async_mode='threading')
def GametouzhuStatByDay():
    """游戏统计投注统计按天"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B174").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A175").options(transpose=True).value = list(
        gameStatBytouzhudata(starttime, endtime).keys())
    wb.sheets["用户数据统计"].range("B175").options(transpose=True).value = list(
        gameStatBytouzhudata(starttime, endtime).values())


@xw.func(async_mode='threading')
def GametouzhuStatByMonth():
    """游戏统计投注统计按月"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B194").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A195").options(transpose=True).value = list(
        gameStatBytouzhudata(startMonth, endMonth).keys())
    wb.sheets["用户数据统计"].range("B195").options(transpose=True).value = list(
        gameStatBytouzhudata(startMonth, endMonth).values())


@xw.func(async_mode='threading')
def GamePlayerStatByDay():
    """玩家数据统计按天"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B214").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A215").options(transpose=True).value = list(
        gameStatByPlayerdata(starttime, endtime).keys())
    wb.sheets["用户数据统计"].range("B215").options(transpose=True).value = list(
        gameStatByPlayerdata(starttime, endtime).values())


@xw.func(async_mode='threading')
def GamePlayerStatByMonth():
    """玩家数据统计按月"""
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B234").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("A235").options(transpose=True).value = list(
        gameStatByPlayerdata(startMonth, endMonth).keys())
    wb.sheets["用户数据统计"].range("B235").options(transpose=True).value = list(
        gameStatByPlayerdata(startMonth, endMonth).values())
