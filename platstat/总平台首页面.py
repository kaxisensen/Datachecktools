import xlwings as xw
from datetime import datetime, timedelta
import operator
from platstat import miscellaneous

db = miscellaneous.mongo_auth()
starttime = miscellaneous.starttime
endtime = miscellaneous.endtime
excludehalllist = miscellaneous.excludehalllist


def total_bet_score(sdate, edate):
    '''
    统计总平台首页面一天之内的总投注额
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回当天总投注额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist}, "is_cancel": {"$eq": 0}}},
            {"$group": {"_id": "null", "总投注": {"$sum": "$total_bet_score"}}}
        ]
    )
    co = list(total).copy()
    if bool(co):
        return list(co)[0]["总投注"]
    else:
        return 0


def operator_win_score(sdate, edate):
    '''
    统计总平台首页面一天之内的总派彩额
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回总平台首页面一天之内的总派彩额
    '''
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist}, "is_cancel": {"$eq": 0}}},
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


def loginTerminalstatistics(sdate, endate):
    '''
    总平台首页面渠道统计
    :param sdate: 开始时间
    :param endate: 结束时间
    :return: 返回首页面渠道统计
    '''
    total = db.get_collection("login_log").aggregate(
        [
            {"$match": {"add_time": {"$gte": sdate,
                                     "$lte": endate},
                        "hall_id": {"$nin": excludehalllist}}},
            {"$group": {"_id": "$user_name", "登录终端": {"$first": "$device_type"}}}
        ])
    logintype = []
    for i in total:
        logintype.append(i["登录终端"])
    return {"PC_H5": logintype.count('PC_H5'), "PC_FLASH": logintype.count('PC_FLASH'), "H5": logintype.count('H5'),
            "NEW_APP": logintype.count('NEW_APP')}


# 总平台首页面今日注单数
betcount = lambda: db.get_collection("user_order").find(
    {"add_time": {"$gte": starttime,
                  "$lte": endtime}, "status": 4, "is_cancel": {"$ne": 1},
     "hall_id": {
         "$nin": excludehalllist
     }}).count()


def hallProfitTop10(sdate, edate):
    '''
    总平台首页面厅主盈利排行【首次下注后取消注单的不算】
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回首页面厅主盈利排行
    '''
    hallprofit = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_id":
                    {"$nin": excludehalllist}
                , "end_time": {"$gte": sdate,
                               "$lte": edate}, "is_cancel": {"$eq": 0}
            }
        }, {"$project": {"hall_name": 1, "total_win_score": 1}},
        {"$group": {"_id": {"厅主编号": "$hall_id", "厅主名称": "$hall_name"}, "合计": {"$sum": "$total_win_score"}}},
        {"$sort": {"合计": 1}},
        {"$limit": 10}
    ])
    co = list(hallprofit).copy()
    li = dict()
    if bool(co):
        for i in co:
            li.update({i['_id']['厅主名称']: i['合计']})
        return li
    else:
        return {"hall_none": 0}


def hallActiveMemberTop10(sdate, edate):
    '''
    总平台首页面厅主活跃会员数排名
    :param sdate: 开始日期
    :param edate: 结束日期
    :return: 返回厅主活跃会员数排名
    '''
    acitvemember = db.get_collection("user_chart_info").aggregate([
        {
            "$match": {
                "hall_id":
                    {"$nin": excludehalllist}
                , "end_time": {"$gte": sdate,
                               "$lte": edate}, "is_cancel": {"$eq": 0}
            }
        },
        {"$group": {"_id": {"厅主编号": "$hall_id", "厅主名称": "$hall_name"}, "会员首次下注": {"$addToSet": "$user_name"}}},
        {"$limit": 10}
    ])
    co = list(acitvemember).copy()
    li = dict()
    if bool(co):
        for i in co:
            li.update({i['_id']['厅主名称']: len(i['会员首次下注'])})
        return dict(sorted(li.items(), key=operator.itemgetter(1), reverse=True))
    else:
        return {'player_none': 0}


@xw.func(async_mode='threading')
def platFirstPage():
    wb = xw.Book.caller()
    wb.sheets["首页面"].range("B1").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["首页面"].range("C3").value = operator_win_score(starttime, endtime)
    wb.sheets["首页面"].range("C4").value = total_bet_score(starttime, endtime)
    wb.sheets["首页面"].range("B6").options(transpose=True).value = list(
        loginTerminalstatistics(starttime, endtime).keys())
    wb.sheets["首页面"].range("C6").options(transpose=True).value = list(
        loginTerminalstatistics(starttime, endtime).values())
    wb.sheets["首页面"].range("C11").value = betcount()
    wb.sheets["首页面"].range("A18").options(transpose=True).value = list(
        range(1, len(hallProfitTop10(starttime, endtime)) + 1))
    wb.sheets["首页面"].range("B18").options(transpose=True).value = list(hallProfitTop10(starttime, endtime).keys())
    wb.sheets["首页面"].range("C18").options(transpose=True).value = list(hallProfitTop10(starttime, endtime).keys())
    wb.sheets["首页面"].range("A31").options(transpose=True).value = list(
        range(1, len(hallActiveMemberTop10(starttime, endtime)) + 1))
    wb.sheets["首页面"].range("B31").options(transpose=True).value = list(hallActiveMemberTop10(starttime, endtime).keys())
    wb.sheets["首页面"].range("C31").options(transpose=True).value = list(
        hallActiveMemberTop10(starttime, endtime).values())
