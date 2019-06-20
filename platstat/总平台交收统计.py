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


def agentOfHallProfit(hallName, sdate, edate):
    '''
    统计指定厅主下的代理的盈利情况
    :param hallName: 厅主名称
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回指定厅主下的各代理的盈利情况
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [{
            "$match": {
                "end_time": {
                    "$gte": sdate,
                    "$lte": edate
                },
                "hall_name": hallName, "is_cancel": {"$eq": 0}
            }
        },
            {"$group": {"_id": "$agent_name", "游戏盈利": {"$sum": "$operator_win_score"}}},
            {"$sort": {"agent_name": 1}}
        ]
    )
    co = list(total).copy()
    if bool(co):
        return co
    else:
        return ['None', 0]


@xw.func(async_mode='threading')
def deliveryAgentOfHallProfit():
    wb = xw.Book.caller()
    wbc = xw.Book(executeFile)
    getHallname = wbc.sheets["交收系统"].range("G1").value
    sDate = wbc.sheets["交收系统"].range("I1").value
    eDate = wbc.sheets["交收系统"].range("J1").value
    sDate = datetime.strptime(sDate, '%Y-%m-%d %H:%M:%S') + timedelta(hours=4)
    eDate = datetime.strptime(eDate, '%Y-%m-%d %H:%M:%S') + timedelta(hours=4)
    for i, v in enumerate(agentOfHallProfit(getHallname, sDate, eDate), 1):
        wb.sheets["交收系统"].range("F" + str(6 + i)).value = [v['_id'], v['游戏盈利']]
