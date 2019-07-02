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


def gameProfitEarlywarning(sdate, edate):
    '''
    总平台风险控制游戏桌盈利排行
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
