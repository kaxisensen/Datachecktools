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


def playerlogin(loginsdate, loginedate):
    '''
    设备统计
    :param loginsdate: 登录开始日期
    :param loginedate: 登录结束日期
    :return: 返回指定日期登录设备统计
    '''
    logintype = []
    total = db.get_collection("login_log").aggregate(
        [
            {"$match": {"add_time": {"$gte": loginsdate,
                                     "$lte": loginedate},
                        "hall_id": {"$nin": excludehalllist}}},
            {"$group": {"_id": "$user_name", "登录终端": {"$first": "$device_type"}}},
            {"$project": {"登录终端": 1, "_id": 0}}
        ])
    for i in total:
        logintype.append(i["登录终端"])
    return {"PC_H5": logintype.count('PC_H5'), "PC_FLASH": logintype.count('PC_FLASH'), "H5": logintype.count('H5'),
            "NEW_APP": logintype.count('NEW_APP')}


def monthOfplayerlogin():
    '''
    总平台用户数据统计的登录玩家统计之月统计
    :return:返回一个月内玩家登录设备统计
    '''
    count = {'PC_H5': 0, 'PC_FLASH': 0, 'H5': 0, 'NEW_APP': 0}
    for sday, eday in daysOfMonth:
        for v, k in playerlogin(sday, eday).items():
            count[v] += k
    return count


def activePlayer(sdate, edate):
    '''
    总平台用户数据统计之设备统计一天或一月之内的活跃玩家
    :param sdate: 开始时间
    :param edate: 结束时间
    :return: 返回用户数据统计之设备统计一天或一月之内的活跃玩家
    '''
    activetype = []
    total = db.get_collection("user_chart_info").aggregate(
        [
            {"$match": {"end_time": {"$gte": sdate,
                                     "$lte": edate},
                        "hall_id": {"$nin": excludehalllist}}},
            {"$group": {"_id": {"user_name": "$user_name", "login_type": "$login_type"},
                        "合计": {"$first": "$login_type"}}},
            {"$project": {"合计": 1, "_id": 0}}
        ])
    for i in total:
        if i['合计'] == 0:
            i['合计'] = 'PC_FLASH'
        elif i['合计'] == 1 or i['合计'] == 2 or i['合计'] == 5 or i['合计'] == 6:
            i['合计'] = 'NEW_APP'
        elif i['合计'] == 3:
            i['合计'] = 'H5'
        else:
            i['合计'] = 'PC_H5'
        activetype.append(i['合计'])
    return {"PC_H5": activetype.count('PC_H5'), "PC_FLASH": activetype.count('PC_FLASH'), "H5": activetype.count('H5'),
            "NEW_APP": activetype.count('NEW_APP')}


def monthOfactivePlayer():
    '''
    总平台用户数据统计的活跃玩家统计之月统计
    :return:返回用户数据统计的活跃玩家统计之月统计
    '''
    count = {'PC_H5': 0, 'PC_FLASH': 0, 'H5': 0, 'NEW_APP': 0}
    for sday, eday in daysOfMonth:
        for v, k in activePlayer(sday, eday).items():
            count[v] += k
    return count


@xw.func(async_mode='threading')
def loginStat():
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("B80").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range('B81').options(transpose=True).value = list(playerlogin(starttime, endtime).keys())
    wb.sheets["用户数据统计"].range('C81').options(transpose=True).value = list(playerlogin(starttime, endtime).values())
    wb.sheets["用户数据统计"].range("B85").value = datetime.utcnow() - timedelta(hours=4)
    monthStat = monthOfplayerlogin()
    wb.sheets["用户数据统计"].range('B86').options(transpose=True).value = list(monthStat.keys())
    wb.sheets["用户数据统计"].range('C86').options(transpose=True).value = list(monthStat.values())


@xw.func(async_mode='threading')
def activeStat():
    wb = xw.Book.caller()
    wb.sheets["用户数据统计"].range("k80").value = datetime.utcnow() - timedelta(hours=4)
    wb.sheets["用户数据统计"].range('k81').options(transpose=True).value = list(activePlayer(starttime, endtime).keys())
    wb.sheets["用户数据统计"].range('M81').options(transpose=True).value = list(activePlayer(starttime, endtime).values())
    wb.sheets["用户数据统计"].range("k85").value = datetime.utcnow() - timedelta(hours=4)
    monthactiveStat = monthOfactivePlayer()
    wb.sheets["用户数据统计"].range('k86').options(transpose=True).value = list(monthactiveStat.keys())
    wb.sheets["用户数据统计"].range('M86').options(transpose=True).value = list(monthactiveStat.values())
