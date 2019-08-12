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
executeFile = miscellaneous.executeFile


def gameProfitEarlywarning(sdate, edate):
    '''
    总平台风险控制游戏桌盈利排行，更新内容
    :param sdate:开始时间
    :param edate:结束时间
    :return:返回当天所有游戏的盈利情况，盈利值相对于玩家
    '''
    gameProfit = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_id": {
                    "$nin": excludehalllist
                },
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"游戏厅": "$game_hall_id", "游戏名称": "$game_name", "桌号": "$server_name"},
                    "盈利": {"$sum": "$operator_win_score"},
                    "下注总额": {"$sum": "$total_bet_score"},
                    "注单数": {"$sum": 1}}},
        {"$sort": {"盈利": -1}}
    ])
    co = list(gameProfit).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'游戏厅': 0, '游戏名称': None, '桌号': None}, '盈利': 0, '下注总额': 0, '注单数': 0}]


def playProfitList(sdate=starttime, edate=endtime):
    '''
    玩家盈利榜
    :param sdate:开始时间,默认为当天美东开始时间
    :param edate:结束时间,默认为当天美东结束时间
    :return:根据时间段选择返回玩家盈利榜
    '''
    profitlist = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_id": {
                    "$nin": excludehalllist
                },
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": "$user_name", "赢钱金额": {"$sum": "$total_win_score"}}},
        {"$limit": 20},
        {"$sort": {"赢钱金额": -1}}
    ])
    co = list(profitlist).copy()
    d = {}
    if bool(co):
        for i in co:
            d.update({i['_id']: i['赢钱金额']})
        return d
    else:
        return {"None": 0}


def playerWinningList(sdate=starttime, edate=endtime):
    '''
    玩家连胜榜
    :param sdate:开始时间,默认为当天美东开始时间
    :param edate:结束时间,默认为当天美东结束时间
    :return:根据时间段选择返回玩家连胜榜
    '''
    winninglist = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_id": {
                    "$nin": excludehalllist
                },
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": "$user_name", "total_win_score": {"$push": {"title": "$total_win_score"}}}},
        {"$limit": 20},
        {"$sort": {"end_time": -1}}
    ])
    co = list(winninglist).copy()
    if bool(co):
        wins = {}
        for i in co:
            maxvictory = []
            victory = 0
            for j in i["total_win_score"]:
                if j['title'] > 0:
                    victory += 1
                else:
                    maxvictory.append(victory)
                    victory = 0
            if maxvictory:
                wins.update({i['_id']: max(maxvictory)})
        return dict(sorted(wins.items(), key=lambda x: x[1], reverse=True))
    else:
        return {"None", 0}


wbc = xw.Book(executeFile)
custStarttime = wbc.sheets["总平台风险控制"].range("I51").value
custEndtime = wbc.sheets["总平台风险控制"].range("K51").value
custStarttime = starttime if not custStarttime else datetime.strptime(custStarttime, '%Y-%m-%d %H:%M:%S') + timedelta(
    hours=4)
custEndtime = endtime if not custEndtime else datetime.strptime(custEndtime, '%Y-%m-%d %H:%M:%S') + timedelta(hours=4)


# custEndtime=datetime.strptime(custEndtime,'%Y-%m-%d %H:%M:%S')+ timedelta(hours=4)
# print(custStarttime,custEndtime)
# print(playerWinningList(custStarttime,custEndtime))
@xw.func(async_mode='threading')
def tableOrder():
    wb = xw.Book.caller()
    wb.sheets["总平台风险控制"].range("B1").value = datetime.utcnow() - timedelta(hours=4)
    cell = 5
    for i in gameProfitEarlywarning(starttime, endtime):
        wb.sheets["总平台风险控制"].range('A' + str(cell)).value = [
            miscellaneous.gameHallName(i['_id']['游戏厅']) + i['_id']['游戏名称'] + i['_id']['桌号'], i['盈利'], i['下注总额'],
            i['注单数']]
        cell += 1


@xw.func(async_mode='threading')
def profitList():
    wb = xw.Book.caller()
    wb.sheets["总平台风险控制"].range("A53").options(transpose=True).value = [i + 1 for i in range(21)]
    wb.sheets["总平台风险控制"].range("B53").options(transpose=True).value = list(
        playProfitList(custStarttime, custEndtime).keys())
    wb.sheets["总平台风险控制"].range("C53").options(transpose=True).value = list(
        playProfitList(custStarttime, custEndtime).values())


@xw.func(async_mode='threading')
def liansheng():
    wb = xw.Book.caller()
    wb.sheets["总平台风险控制"].range("D53").options(transpose=True).value = list(
        playerWinningList(custStarttime, custEndtime).keys())
    wb.sheets["总平台风险控制"].range("E53").options(transpose=True).value = list(
        playerWinningList(custStarttime, custEndtime).values())



