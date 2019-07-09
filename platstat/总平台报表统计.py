import xlwings as xw
from platstat import miscellaneous

db = miscellaneous.mongo_auth()
starttime = miscellaneous.starttime
endtime = miscellaneous.endtime
startMonth = miscellaneous.startMonth
endMonth = miscellaneous.endMonth
excludehalllist = miscellaneous.excludehalllist
daysOfMonth = miscellaneous.daysOfMonth


def userGameReport(sdate, edate):
    '''
    总平台查询游戏
    :param sdate:开始时间
    :param edate:结束时间
    :return:返回所有厅主下的所有游戏的投注及盈利情况
    '''
    gameReport = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "is_cancel": {"$eq": 0},
                "end_time": {"$gte": sdate,
                             "$lte": edate}
            }
        },
        {"$group": {"_id": {"游戏": "$game_name"}, "笔数": {"$sum": 1},
                    "有效投注": {"$sum": "$valid_bet_score_total"}, "总下注金额": {"$sum": "$total_bet_score"},
                    "盈利": {"$sum": "$operator_win_score"}}}
    ])
    co = list(gameReport).copy()
    if bool(co):
        return co
    else:
        return [{'_id': {'游戏': 'None'}, '笔数': 0, '有效投注': 0, '总下注金额': 0, '盈利': 0}]


@xw.func(async_mode='threading')
def GameReport():
    wb = xw.Book.caller()
    for i, v in enumerate(userGameReport(starttime, endtime), 1):
        wb.sheets["报表统计"].range("A" + str(2 + i)).value = [v['_id']['游戏'], v['笔数'], v['有效投注'],
                                                           v['总下注金额'], v['盈利']]
