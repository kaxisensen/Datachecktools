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


def pltHallStat(startDate, endDate, hallName):
    '''
        总平台用户数据统计之厅主统计一天之内的下注金额与派彩金额
    :param startDate: 开始日期
    :param endDate: 结束日期
    :param hallName: 厅主名称
    :return:返回总平台用户数据统计之厅主统计一天之内的下注金额与派彩金额
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
                "hall_name": {"$eq": hallName}
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


# 总平台用户数据统计之厅主统计一天之内的注单数
betcount = lambda startdate, enddate, hallID: db.get_collection("user_order").find(
    {"add_time": {"$gte": startdate,
                  "$lte": enddate}, "status": 4, "is_cancel": {"$ne": 1},
     "hall_id": {
         "$nin": excludehalllist, "$eq": hallID
     }}).count()


@xw.func(async_mode='threading')
def hallStat():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    getHallname = wbc.sheets["用户数据统计"].range("G49").value
    getHallID = int(wbc.sheets["用户数据统计"].range("G50").value)
    wb.sheets["用户数据统计"].range("B48").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("B49").options(transpose=True).value = list(
        pltHallStat(starttime, endtime, getHallname).keys())
    wb.sheets["用户数据统计"].range("C49").options(transpose=True).value = list(
        pltHallStat(starttime, endtime, getHallname).values())
    wb.sheets["用户数据统计"].range("B52").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("B53").options(transpose=True).value = list(
        pltHallStat(startMonth, endMonth, getHallname).keys())
    wb.sheets["用户数据统计"].range("C53").options(transpose=True).value = list(
        pltHallStat(startMonth, endMonth, getHallname).values())
    wb.sheets["用户数据统计"].range("B56").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C57").value = betcount(starttime, endtime, getHallID)
    wb.sheets["用户数据统计"].range("B59").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range("C60").value = betcount(startMonth, endMonth, getHallID)
