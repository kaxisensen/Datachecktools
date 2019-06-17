import xlwings as xw
from datetime import datetime, timedelta
from platstat import miscellaneous

db = miscellaneous.mongo_auth()
starttime = miscellaneous.starttime
endtime = miscellaneous.endtime
startMonth = miscellaneous.startMonth
endMonth = miscellaneous.endMonth
excludehalllist = miscellaneous.excludehalllist
executeFile = miscellaneous.executeFile


def pltAgentStat(startDate, endDate, agentName):
    '''
    总平台用户数据统计之代理统计一天或一月之内的下注金额与派彩金额
    :param startDate: 开始日期
    :param endDate: 结束日期
    :param agentName: 代理名称
    :return:返回总平台用户数据统计之代理统计一天或一月之内的下注金额与派彩金额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [{
            "$match": {
                "end_time": {
                    "$gte": startDate,
                    "$lte": endDate
                },
                "hall_id": {
                    "$nin": excludehalllist
                }, "is_cancel": {"$eq": 0},
                "agent_name": {"$eq": agentName}
            }
        },
            {"$group": {"_id": "null", "盈利总额": {"$sum": "$operator_win_score"}, "投注总额": {"$sum": "$total_bet_score"}}},
            {"$project": {"_id": 0, "盈利总额": 1, "投注总额": 1}}
        ]
    )
    co = list(total).copy()
    if bool(co):
        return co[0]
    else:
        return {'盈利总额': 0, '投注总额': 0}


# 总平台用户数据统计之代理统计一天或一月之内的注单数
betcount = lambda startdate, enddate, agentID: db.get_collection("user_order").find(
    {"add_time": {"$gte": startdate,
                  "$lte": enddate}, "status": 4, "is_cancel": {"$ne": 1},
     "hall_id": {
         "$nin": excludehalllist
     }, "agent_id": {"$eq": agentID}}).count()


@xw.func(async_mode='threading')
def agentStat():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    getAgentname = wbc.sheets["用户数据统计"].range("G63").value
    getAgentID = int(wbc.sheets["用户数据统计"].range("G64").value)
    wb.sheets["用户数据统计"].range("B64").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("B65").options(transpose=True).value = list(
        pltAgentStat(starttime, endtime, getAgentname).keys())
    wb.sheets["用户数据统计"].range("C65").options(transpose=True).value = list(
        pltAgentStat(starttime, endtime, getAgentname).values())
    wb.sheets["用户数据统计"].range("B68").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("B69").options(transpose=True).value = list(
        pltAgentStat(startMonth, endMonth, getAgentname).keys())
    wb.sheets["用户数据统计"].range("C69").options(transpose=True).value = list(
        pltAgentStat(startMonth, endMonth, getAgentname).values())
    wb.sheets["用户数据统计"].range("B72").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C73").value = betcount(starttime, endtime, getAgentID)
    wb.sheets["用户数据统计"].range("B75").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C76").value = betcount(startMonth, endMonth, getAgentID)

