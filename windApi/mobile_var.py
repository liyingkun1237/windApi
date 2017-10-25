# -*- coding: utf-8 -*-
# """
# Created on Fri Jul  7 17:45:24 2017
#
# @author: wjj
#
# 此脚本为计算base相关变量的函数，创建人wangjiujun@ccx.cn
#
# 原始脚本整理至：C:\Users\Administrator\Desktop\tn_getmobilevars\get_mobilevars.py
# """

import pandas as pd
import numpy as np
# from tn.log import tn_log


# '''
# df:通话详单原始的详单数据
# '''
# @tn_log('get_mobilevars')
def get_mobilevars(df):
    #    vars_dict = { }

    # 处理分母为0的情况
    def div_fun(val1, val2):
        if val2 != 0:
            result = val1 / val2
        else:
            result = 0
        return result

    callnum_cnt = len(df.drop_duplicates(['other_number']))
    totalcall_cnt = len(df)

    # add three new columns:nativeorinte,roam_call,provordome
    df.loc[(df.land_type.notnull()) & (df.land_type.str.contains('本地|市话')), 'nativeorinte'] = '本地通话'
    df.loc[(df.land_type.notnull()) & (df.land_type.str.contains('国际')), 'nativeorinte'] = '国际通话'
    df.loc[(df.land_type.notnull()) & (df.land_type.str.contains('漫游')), 'roam_call'] = '漫游通话'

    # 本地、省内、省际、漫游等通话次数，和平均每次通话时间
    if ('本地通话' in df.nativeorinte.values):  # 判断是否存在本地通话
        nativecall_cnt = len(df.loc[df.nativeorinte == '本地通话'])
        nativecalldur_avg = div_fun(df.loc[df.nativeorinte == '本地通话'].times.sum(), nativecall_cnt)  # 本地平均每次通话时间
    else:
        nativecall_cnt = 0
        nativecalldur_avg = 0

    if ('国际通话' in df.nativeorinte.values):  # 判断是否存在国际通话
        has_abrcall = 1
    else:
        has_abrcall = 0

    if (df.roam_call.count() != 0):  # 判断是否存在漫游通话 '漫游通话' in df.roam_call.values 0712修改
        roamcall_cnt = len(df.loc[df.roam_call == '漫游通话'])
        roamcalldur_avg = div_fun(df.loc[df.roam_call == '漫游通话'].times.sum(), roamcall_cnt)  # 漫游平均每次通话时间
    else:
        roamcall_cnt = 0
        roamcalldur_avg = 0

    df.loc[(df.nativeorinte.isnull()) & (df.land_type.str.contains('省内')), 'provordome'] = '省内通话'
    fun_dome = lambda x: ('省内' not in x)
    df.loc[(df.nativeorinte.isnull()) & (df.land_type.map(fun_dome)), 'provordome'] = '省际通话'

    if ('省内通话' in df.provordome.values):  # 判断是否存在省内通话
        provcall_cnt = len(df.loc[df.provordome == '省内通话'])
        procalldur_avg = div_fun(df.loc[df.provordome == '省内通话'].times.sum(), provcall_cnt)  # 省内平均每次通话时间
    else:
        provcall_cnt = 0
        procalldur_avg = 0

    if ('省际通话' in df.provordome.values):  # 判断是否存在省际通话
        domcall_cnt = len(df.loc[df.provordome == '省际通话'])
        domecalldur_avg = div_fun(df.loc[df.provordome == '省际通话'].times.sum(), domcall_cnt)
    else:
        domcall_cnt = 0
        domecalldur_avg = 0

    # the following vars are obtained by statistical approach
    total_times = df.times.sum()
    if (len(df.other_number.value_counts()) >= 3):  # 判断是否有多于三个通话
        top3callnum_cnt_ratio = df.other_number.value_counts()[0:3].sum() / totalcall_cnt
        top3call = df.loc[df.other_number.isin(list(df.other_number.value_counts()[0:3].index))]
        top3callnum_dur_ratio = div_fun(top3call.times.sum(), total_times)
    else:
        top3callnum_cnt_ratio = np.nan
        top3callnum_dur_ratio = np.nan
    longest_dur = df.times.max()

    # 通话最频繁的城市和通话地点个数
    mostfreq_city = df.call_address.value_counts().index[0]  # .index[0]是排名最靠前的城市
    callloc_num = len(df.call_address.value_counts())

    latestcall_diffdays = ((df.inquiry_time - df.stat_time) / np.timedelta64(1, 'D')).min()  # 在时间差中取最小值

    # 不同条件下的月均通话次数、通话时长
    start_time_month = df.stat_time.dt.strftime('%Y-%m')
    df['start_time_month'] = start_time_month
    delta_month = len(start_time_month.value_counts())  # 电话使用时间（月份）对每个变量来说是相同的，并且，肯定不为0

    callcnt_mth_avg = len(df) / delta_month
    calldur_mth_avg = total_times / delta_month  # 总通话时长与手机使用月份之比

    # 平均每次通话时长
    calldur_avg = div_fun(total_times, totalcall_cnt)

    # 主叫次数、主叫月均通话次数、主叫月均通话时长，月最多主叫次数，主叫平均每次通话时长
    if (1 in df.call_channel.values):  # 主叫
        origcall_cnt = len(df[df['call_channel'] == 1])
        origcallcnt_mth_avg = len(df.loc[df.call_channel == 1]) / delta_month
        origcalldur_mth_avg = df.loc[df.call_channel == 1].times.sum() / delta_month
        origcallcnt_mth_max = df.loc[df.call_channel == 1].start_time_month.describe().freq
        origcalldur_avg = div_fun(df.loc[df.call_channel == 1].times.sum(), origcall_cnt)
    else:
        origcall_cnt = 0
        origcallcnt_mth_avg = 0
        origcalldur_mth_avg = 0
        origcallcnt_mth_max = 0
        origcalldur_avg = 0
    # 被叫次数、被叫月均通话次数、主叫月均通话时长，月最多被叫次数，被叫平均每次通话时长
    if (2 in df.call_channel.values):  # 被叫
        termcall_cnt = len(df[df['call_channel'] == 2])
        termcallcnt_mth_avg = len(df.loc[df.call_channel == 2]) / delta_month
        termcalldur_mth_avg = df.loc[df.call_channel == 2].times.sum() / delta_month
        termcallcnt_mth_max = df.loc[df.call_channel == 2].start_time_month.describe().freq
        termcalldur_avg = div_fun(df.loc[df.call_channel == 2].times.sum(), termcall_cnt)
    else:
        termcall_cnt = 0
        termcallcnt_mth_avg = 0
        termcalldur_mth_avg = 0
        termcallcnt_mth_max = 0
        termcalldur_avg = 0

    # 月最多、最少通话次数
    callcnt_mth_max = start_time_month.describe().freq
    callcnt_mth_min = start_time_month.value_counts().min()

    # 本地、省内、省际+主叫、被叫+通话次数、平均每次通话时长、平均每月通话次数、平均每月通话时长

    if ('本地通话' in df.nativeorinte.values):  # 判断是否存在本地通话
        nativecallcnt_mth_avg = len(df.loc[df.nativeorinte == '本地通话']) / delta_month
        nativecalldur_mth_avg = df.loc[df.nativeorinte == '本地通话'].times.sum() / delta_month

        if (1 in df.call_channel.values):  # 判断是否存在主叫
            native_orig_cnt = len(df.loc[(df.nativeorinte == '本地通话') & (df.call_channel == 1)])  # 本地主叫通话次数
            nativeorigdur = df.loc[(df.nativeorinte == '本地通话') & (df.call_channel == 1)].times.sum()
            nativeorigdur_avg = div_fun(nativeorigdur, native_orig_cnt)
            nativeorigcnt_mth_avg = native_orig_cnt / delta_month  # 本地主叫平均每月通话次数
            nativeorigdur_mth_avg = df.loc[(df.nativeorinte == '本地通话') & (
                df.call_channel == 1)].times.sum() / delta_month  # 平均每月通话时长
        else:
            native_orig_cnt = 0
            nativeorigdur_avg = 0
            nativeorigcnt_mth_avg = 0
            nativeorigdur_mth_avg = 0

        if (2 in df.call_channel.values):  # 判断是否存在被叫
            native_term_cnt = len(df.loc[(df.nativeorinte == '本地通话') & (df.call_channel == 2)])  # 本地被叫通话次数
            nativetermdur = df.loc[(df.nativeorinte == '本地通话') & (df.call_channel == 2)].times.sum()
            nativetermdur_avg = div_fun(nativetermdur, native_term_cnt)
            nativetermcnt_mth_avg = native_term_cnt / delta_month  # 本地被叫平均每月通话次数
            nativetermdur_mth_avg = df.loc[(df.nativeorinte == '本地通话') & (
                df.call_channel == 2)].times.sum() / delta_month  # 平均每月通话时长
        else:
            native_term_cnt = 0
            nativetermdur_avg = 0
            nativetermcnt_mth_avg = 0
            nativetermdur_mth_avg = 0

    else:
        nativecallcnt_mth_avg = 0
        nativecalldur_mth_avg = 0
        native_orig_cnt = 0
        nativeorigdur_avg = 0
        nativeorigcnt_mth_avg = 0
        nativeorigdur_mth_avg = 0
        native_term_cnt = 0
        nativetermdur_avg = 0
        nativetermcnt_mth_avg = 0
        nativetermdur_mth_avg = 0

    if ('省内通话' in df.provordome.values):
        provcallcnt_mth_avg = len(df.loc[df.provordome == '省内通话']) / delta_month
        provcalldur_mth_avg = df.loc[df.provordome == '省内通话'].times.sum() / delta_month

        if (1 in df.call_channel.values):  # 判断是否存在主叫
            prov_orig_cnt = len(df.loc[(df.provordome == '省内通话') & (df.call_channel == 1)])  # 省内主叫通话次数
            provorigdur = df.loc[(df.provordome == '省内通话') & (df.call_channel == 1)].times.sum()
            provorigdur_avg = div_fun(provorigdur, prov_orig_cnt)
            provorigcnt_mth_avg = prov_orig_cnt / delta_month  # 平均每月通话次数
            provorigdur_mth_avg = df.loc[(df.provordome == '省内通话') & (
                df.call_channel == 1)].times.sum() / delta_month  # 平均每月通话时长
        else:
            prov_orig_cnt = 0
            provorigdur_avg = 0
            provorigcnt_mth_avg = 0
            provorigdur_mth_avg = 0

        if (2 in df.call_channel.values):  # 判断是否存在被叫
            prov_term_cnt = len(df.loc[(df.provordome == '省内通话') & (df.call_channel == 2)])  # 省内被叫通话次数
            provtermdur = df.loc[(df.provordome == '省内通话') & (df.call_channel == 2)].times.sum()
            provtermdur_avg = div_fun(provtermdur, prov_term_cnt)
            provtermcnt_mth_avg = prov_term_cnt / delta_month  # 平均每月通话次数
            provtermdur_mth_avg = df.loc[(df.provordome == '省内通话') & (
                df.call_channel == 2)].times.sum() / delta_month  # 平均每月通话时长
        else:
            prov_term_cnt = 0
            provtermdur_avg = 0
            provtermcnt_mth_avg = 0
            provtermdur_mth_avg = 0

    else:
        provcallcnt_mth_avg = 0
        provcalldur_mth_avg = 0
        prov_orig_cnt = 0
        provorigdur_avg = 0
        provorigcnt_mth_avg = 0
        provorigdur_mth_avg = 0
        prov_term_cnt = 0
        provtermdur_avg = 0
        provtermcnt_mth_avg = 0
        provtermdur_mth_avg = 0

    if ('省际通话' in df.provordome.values):
        domecallcnt_mth_avg = len(df.loc[df.provordome == '省际通话']) / delta_month
        domecalldur_mth_avg = df.loc[df.provordome == '省际通话'].times.sum() / delta_month

        if (1 in df.call_channel.values):  # 判断是否存在主叫
            dome_orig_cnt = len(df.loc[(df.provordome == '省际通话') & (df.call_channel == 1)])  # 省际主叫通话次数
            domeorigdur = df.loc[(df.provordome == '省际通话') & (df.call_channel == 1)].times.sum()
            domeorigdur_avg = div_fun(domeorigdur, dome_orig_cnt)
            domeorigcnt_mth_avg = dome_orig_cnt / delta_month  # 平均每月通话次数
            domeorigdur_mth_avg = df.loc[(df.provordome == '省际通话') & (
                df.call_channel == 1)].times.sum() / delta_month  # 平均每月通话时长
        else:
            dome_orig_cnt = 0
            domeorigdur_avg = 0
            domeorigcnt_mth_avg = 0
            domeorigdur_mth_avg = 0

        if (2 in df.call_channel.values):  # 判断是否存在被叫
            dome_term_cnt = len(df.loc[(df.provordome == '省际通话') & (df.call_channel == 2)])  # 省际被叫通话次数
            dometermdur = df.loc[(df.provordome == '省际通话') & (df.call_channel == 2)].times.sum()
            dometermdur_avg = div_fun(dometermdur, dome_term_cnt)
            dometermcnt_mth_avg = dome_term_cnt / delta_month  # 平均每月通话次数
            dometermdur_mth_avg = df.loc[(df.provordome == '省际通话') & (
                df.call_channel == 2)].times.sum() / delta_month  # 平均每月通话时长
        else:
            dome_term_cnt = 0
            dometermdur_avg = 0
            dometermcnt_mth_avg = 0
            dometermdur_mth_avg = 0

    else:
        domecallcnt_mth_avg = 0
        domecalldur_mth_avg = 0
        dome_orig_cnt = 0
        domeorigdur_avg = 0
        domeorigcnt_mth_avg = 0
        domeorigdur_mth_avg = 0
        dome_term_cnt = 0
        dometermdur_avg = 0
        dometermcnt_mth_avg = 0
        dometermdur_mth_avg = 0

    # 按白天和夜间统计通话时长等信息
    start_time_minute = df.stat_time.dt.strftime('%H:%M')
    df['start_time_minute'] = start_time_minute

    def day_night_split(time1, time2):

        day_times_sum = df.loc[(df.start_time_minute <= time2) & (df.start_time_minute >= time1)].times.sum()
        total_times_sum = df.times.sum()
        night_times_sum = total_times_sum - day_times_sum

        calldur_mthavg_day = day_times_sum / delta_month
        calldur_mthavg_night = night_times_sum / delta_month  # 夜间月均通话时长
        calldur_night_rate = night_times_sum / (day_times_sum + night_times_sum)  # 夜间通话时长占比

        day_cnt_sum = len(df.loc[(df.start_time_minute <= time2) & (df.start_time_minute >= time1)])
        callcnt_mthavg_day = day_cnt_sum / delta_month

        night_cnt_sum = totalcall_cnt - day_cnt_sum
        callcnt_mthavg_night = night_cnt_sum / delta_month  # 夜间月均通话次数
        callcnt_night_rate = night_cnt_sum / totalcall_cnt  # 夜间通话次数占比
        return night_times_sum, night_cnt_sum, calldur_mthavg_day, calldur_mthavg_night, calldur_night_rate, callcnt_mthavg_day, callcnt_mthavg_night, callcnt_night_rate

    # df.drop('start_time_minute',axis = 1,inplace = True)

    # 调用函数day_night_split（）
    night_times_sum, night_cnt_sum, calldur_mthavg_day, calldur_mthavg_night, calldur_night_rate, callcnt_mthavg_day, callcnt_mthavg_night, callcnt_night_rate = \
        day_night_split('07:00', '19:00')

    # 按work和nonwork统计通话时长等信息
    def work_nonwork_split(time1, time2, time3, time4):
        work_times_sum = df.loc[((df.start_time_minute <= time2) & (df.start_time_minute >= time1)) | \
                                ((df.start_time_minute <= time4) & (df.start_time_minute >= time3))].times.sum()
        # 非工作时间：除了工作时间和半夜2-6点之外的时间
        nonwork_times_sum = df.times.sum() - work_times_sum - df.loc[
            (df.start_time_minute <= '06:00') & (df.start_time_minute >= '02:00')].times.sum()
        work_calldur_mthavg = work_times_sum / delta_month
        nonwork_calldur_mthavg = nonwork_times_sum / delta_month
        nonwork_calldur_rate = nonwork_times_sum / (nonwork_times_sum + work_times_sum)

        work_cnt_sum = len(df.loc[((df.start_time_minute <= time2) & (df.start_time_minute >= time1)) | \
                                  ((df.start_time_minute <= time4) & (df.start_time_minute >= time3))])
        nonwork_cnt_sum = totalcall_cnt - work_cnt_sum - len(df.loc[
                                                                 (df.start_time_minute <= '06:00') & (
                                                                     df.start_time_minute >= '02:00')])

        work_callcnt_mthavg = work_cnt_sum / delta_month
        nonwork_callcnt_mthavg = nonwork_cnt_sum / delta_month
        nonwork_callcnt_rate = nonwork_cnt_sum / totalcall_cnt

        return work_calldur_mthavg, nonwork_calldur_mthavg, nonwork_calldur_rate, work_callcnt_mthavg, nonwork_callcnt_mthavg, nonwork_callcnt_rate

    # 调用函数work_nonwork_split（）
    work_calldur_mthavg, nonwork_calldur_mthavg, nonwork_calldur_rate, work_callcnt_mthavg, nonwork_callcnt_mthavg, nonwork_callcnt_rate = \
        work_nonwork_split('08:00', '12:00', '13:30', '18:30')

    # 按workday和weekend统计通话时长等信息
    workday_times_sum = df.loc[
        (df.stat_time.dt.dayofweek < 5) & (df.stat_time.dt.dayofweek >= 0)].times.sum()  # 工作日总通话时间,0721修改过
    weekday_times_sum = df.times.sum() - workday_times_sum  # 周末通话时间总长

    # 工作日、周末月均通话时长
    workday_calldur_mthavg = workday_times_sum / delta_month
    weekday_calldur_mthavg = weekday_times_sum / delta_month

    # 工作日、周末通话时长占比
    workday_calldur_rate = workday_times_sum / (workday_times_sum + weekday_times_sum)
    weekday_calldur_rate = weekday_times_sum / (workday_times_sum + weekday_times_sum)

    workday_cnt_sum = len(df.loc[(df.stat_time.dt.dayofweek <= 5) & (df.stat_time.dt.dayofweek >= 1)])  # 工作日总通话次数
    weekday_cnt_sum = len(df) - workday_cnt_sum  # 周末通话次数

    # 工作日、周末月均通话次数
    workday_callcnt_mthavg = workday_cnt_sum / delta_month
    weekday_callcnt_mthavg = weekday_cnt_sum / delta_month

    # 工作日、周末通话次数占比
    workday_callcnt_rate = workday_cnt_sum / len(df)
    weekday_callcnt_rate = weekday_cnt_sum / len(df)

    callcnt_over20_mths = len(start_time_month.value_counts()[start_time_month.value_counts() > 20])

    # 从超过5分钟且多于两次的通话数量开始
    times_thres = 5 * 60
    calldur_over5min_cnt = len(df.times[df.times > times_thres])  # 通话超过5分钟的通话次数
    over5m_numbers = df.loc[df.times > times_thres].other_number.value_counts()
    over5m_2t_callcnt = len(over5m_numbers[over5m_numbers > 2])  # 通话超过5mins，并超过2次的号码的数量

    # the parameters for defining closed friends
    # minutes_thres = 15*60#每月超过多少分钟
    # mth_top_thres = 10#每月通话记录前几名（按每个号码当月的通话时长作为标准）
    # mths_number = 3#出现超过几个月的电话

    def obtain_clofri_set():
        grouped = df.groupby(['other_number', 'start_time_month'])[['times']].sum()
        grouped_othernum_month = grouped[grouped.times > 900]
        grouped_othernum_month.reset_index(inplace=True)
        mths_counts = grouped_othernum_month.groupby(['other_number']).size()
        over3mths15min = mths_counts[mths_counts.values >= 3]
        if len(over3mths15min) > 10:
            close_friend_set = list(over3mths15min.index[:10])
        else:
            close_friend_set = list(over3mths15min.index)
        return close_friend_set

    # 调用函数，获得密友集合——获得一个list
    # close_friend_set = obtain_clofri_set(group_data,10,3,times_thres)
    close_friend_set = obtain_clofri_set()
    clofri_cnt = len(close_friend_set)

    # 找到最亲密密友对应的号码和通话时间
    if close_friend_set:
        mostclofri_number = df.loc[df.other_number.isin(close_friend_set)].groupby(['other_number'])[
            'times'].sum().idxmax()
        mostclofri_times_sum = df.loc[df.other_number.isin(close_friend_set)].groupby(['other_number'])[
            'times'].sum().max()
    else:
        mostclofri_number = 0
        mostclofri_times_sum = 0

    mostclofri_calldur_mthavg = mostclofri_times_sum / delta_month
    mostclofri_callcnt_sum = len(df.other_number[df.other_number == mostclofri_number])
    mostclofri_callcnt_mthavg = mostclofri_callcnt_sum / delta_month

    mostclofri_callcnt_day = len(df.loc[df.other_number == mostclofri_number].loc[
                                     (df.start_time_minute <= '19:00') & (df.start_time_minute >= '07:00')])
    mostclofri_callcnt = len(df.loc[df.other_number == mostclofri_number])
    mostclofri_callcnt_night = mostclofri_callcnt - mostclofri_callcnt_day

    mostclofri_calltime_day = df.loc[df.other_number == mostclofri_number].loc[
        (df.start_time_minute <= '19:00') & (df.start_time_minute >= '07:00')].times.sum()
    mostclofri_calltime = df.loc[df.other_number == mostclofri_number].times.sum()
    mostclofri_calltime_night = mostclofri_calltime - mostclofri_calltime_day

    # 最亲密密友的白天、夜晚月均通话时长、次数
    mostclofri_calldur_mthavg_day = mostclofri_calltime_day / delta_month
    mostclofri_calldur_mthavg_night = mostclofri_calltime_night / delta_month
    mostclofri_callcnt_mthavg_day = mostclofri_callcnt_day / delta_month
    mostclofri_callcnt_mthavg_night = mostclofri_callcnt_night / delta_month

    # 密友集合夜间通话时长和次数占比
    clofriset_calltime_day = df.loc[df.other_number.isin(close_friend_set)].loc[
        (df.start_time_minute <= '19:00') & (df.start_time_minute >= '07:00')]. \
        times.sum()
    clofriset_calltime = df.loc[df.other_number.isin(close_friend_set)].times.sum()
    clofriset_calltime_night = clofriset_calltime - clofriset_calltime_day

    clofriset_callcnt_day = len(df.loc[df.other_number.isin(close_friend_set)].loc[
                                    (df.start_time_minute <= '19:00') & (df.start_time_minute >= '07:00')])
    clofriset_callcnt = len(df.loc[df.other_number.isin(close_friend_set)])
    clofriset_callcnt_night = clofriset_callcnt - clofriset_callcnt_day

    clofri_calldur_rate = div_fun(clofriset_calltime_night, night_times_sum)
    clofri_callcnt_rate = div_fun(clofriset_callcnt_night, night_cnt_sum)

    # 最少、中等、较突出通话时长和次数等变量
    number_time_groupA = df.groupby(['other_number'])[['times']].sum()
    min_calldur = number_time_groupA.times.min()
    med_calldur = number_time_groupA.times.median()
    spe_thresA = number_time_groupA.times.mean() + 4 * number_time_groupA.times.std()
    times_spe_number = number_time_groupA.loc[number_time_groupA.times > spe_thresA]
    spe_calldur = len(times_spe_number)

    number_cnt_groupB = pd.DataFrame(df.groupby(['other_number']).size(), columns=['counts'])
    min_callcnt = number_cnt_groupB.counts.min()
    med_callcnt = number_cnt_groupB.counts.median()

    spe_thresB = number_cnt_groupB.counts.mean() + 4 * number_cnt_groupB.counts.std()
    cnt_spe_number = number_cnt_groupB.loc[number_cnt_groupB.counts > spe_thresB]
    spe_callcnt = len(cnt_spe_number)
    insetion_cnt = len(set(times_spe_number.index).intersection(set(cnt_spe_number.index)))

    # 利用穷举法判断是否是holiday
    df['start_time_date'] = df.stat_time.dt.strftime('%Y%m%d')

    holiday_map = {'元旦': (20150101, 20150102, 20150103, 20160101, 20160102, 20160103, \
                          20161231, 20170101, 20170102),
                   '春节': (20150218, 20150219, 20150220, 20150221, 20150222, 20150223, 20150224, \
                          20160207, 20160208, 20160209, 20160210, 20160211, 20160212, 20160213, \
                          20170127, 20170128, 20170129, 20170130, 20170131, 20170201, 20170202),
                   '清明节': (20150404, 20150405, 20150406, 20160402, 20160403, 20160404, \
                           20170402, 20170403, 20170404),
                   '劳动节': (20150501, 20150502, 20150503, 20160430, 20160501, 20160502, \
                           20170429, 20170430, 20170501),
                   '端午节': (20150620, 20150621, 20150622, 20160609, 20160610, 20160611, \
                           20170528, 20170529, 20170530),
                   '中秋国庆': (20150927, 20151001, 20151002, 20151003, 20151004, 20151005, 20151006, 20151007, \
                            20160915, 20160916, 20160917, 20161001, 20161002, 20161003, 20161004, 20161005, 20161006,
                            20161007,
                            20171001, 20171002, 20171003, 20171004, 20171005, 20171006, 20171007, 20171008)
                   }

    holiday_list = []
    for i in holiday_map.keys():
        holiday_list = holiday_list + list(holiday_map[i])

    holiday_date = []  # 将日期由int型转换为str
    for i in holiday_list:
        holiday_date.append(str(i))

    # 获得三个holiday相关的变量，分别是节假日通话次数、节假日通话得点个数、节假日通话时长
    holiday_callcnt = len(df.loc[df.start_time_date.isin(holiday_date)])
    holiday_callloc_cnt = len(df.loc[df.start_time_date.isin(holiday_date)].call_address.value_counts())
    holiday_calldur_mthavg = df.loc[df.start_time_date.isin(holiday_date)].times.sum()

    # df.drop(['start_time_month','start_time_minute'],axis = 1, inplace = True)
    # 整理生成的变量结果
    vars_list = [callnum_cnt, totalcall_cnt, origcall_cnt, termcall_cnt, nativecall_cnt, \
                 roamcall_cnt, provcall_cnt, domcall_cnt, has_abrcall, top3callnum_cnt_ratio, \
                 top3callnum_dur_ratio, longest_dur, calldur_avg, origcalldur_avg, termcalldur_avg, \
                 nativecalldur_avg, procalldur_avg, domecalldur_avg, roamcalldur_avg, mostfreq_city, \
                 callloc_num, latestcall_diffdays, callcnt_mth_avg, origcallcnt_mth_avg, termcallcnt_mth_avg, \
                 nativecallcnt_mth_avg, provcallcnt_mth_avg, domecallcnt_mth_avg, callcnt_mth_max, callcnt_mth_min, \
                 termcallcnt_mth_max, origcallcnt_mth_max, calldur_mth_avg, origcalldur_mth_avg, termcalldur_mth_avg, \
                 nativecalldur_mth_avg, provcalldur_mth_avg, domecalldur_mth_avg, native_orig_cnt, nativeorigdur_avg, \
                 nativeorigcnt_mth_avg, nativeorigdur_mth_avg, native_term_cnt, nativetermdur_avg,
                 nativetermcnt_mth_avg,
                 nativetermdur_mth_avg, prov_orig_cnt, provorigdur_avg, provorigcnt_mth_avg, provorigdur_mth_avg, \
                 prov_term_cnt, provtermdur_avg, provtermcnt_mth_avg, provtermdur_mth_avg, dome_orig_cnt, \
                 domeorigdur_avg, domeorigcnt_mth_avg, domeorigdur_mth_avg, dome_term_cnt, dometermdur_avg, \
                 dometermcnt_mth_avg, dometermdur_mth_avg, calldur_mthavg_day, calldur_mthavg_night, calldur_night_rate,
                 callcnt_mthavg_day, \
                 callcnt_mthavg_night, callcnt_night_rate, work_calldur_mthavg, nonwork_calldur_mthavg,
                 nonwork_calldur_rate, \
                 work_callcnt_mthavg, nonwork_callcnt_mthavg, nonwork_callcnt_rate, workday_calldur_mthavg,
                 weekday_calldur_mthavg, \
                 workday_calldur_rate, weekday_calldur_rate, workday_callcnt_mthavg, weekday_callcnt_mthavg,
                 workday_callcnt_rate, \
                 weekday_callcnt_rate, callcnt_over20_mths, calldur_over5min_cnt, over5m_2t_callcnt, clofri_cnt, \
                 mostclofri_calldur_mthavg, mostclofri_callcnt_mthavg, mostclofri_calldur_mthavg_day, \
                 mostclofri_calldur_mthavg_night, mostclofri_callcnt_mthavg_day, \
                 mostclofri_callcnt_mthavg_night, clofri_calldur_rate, clofri_callcnt_rate, min_calldur, med_calldur, \
                 spe_calldur, min_callcnt, med_callcnt, spe_callcnt, insetion_cnt, holiday_callcnt,
                 holiday_callloc_cnt, \
                 holiday_calldur_mthavg]

    vars_name = ['callnum_cnt', 'totalcall_cnt', 'origcall_cnt', 'termcall_cnt', 'nativecall_cnt', 'roamcall_cnt', \
                 'provcall_cnt', 'domcall_cnt', 'has_abrcall', 'top3callnum_cnt_ratio', 'top3callnum_dur_ratio',
                 'longest_dur', \
                 'calldur_avg', 'origcalldur_avg', 'termcalldur_avg', 'nativecalldur_avg', 'procalldur_avg',
                 'domecalldur_avg', \
                 'roamcalldur_avg', 'mostfreq_city', 'callloc_num', 'latestcall_diffdays', 'callcnt_mth_avg',
                 'origcallcnt_mth_avg', \
                 'termcallcnt_mth_avg', 'nativecallcnt_mth_avg', 'provcallcnt_mth_avg', 'domecallcnt_mth_avg',
                 'callcnt_mth_max', 'callcnt_mth_min', \
                 'termcallcnt_mth_max', 'origcallcnt_mth_max', 'calldur_mth_avg', 'origcalldur_mth_avg',
                 'termcalldur_mth_avg', 'nativecalldur_mth_avg', \
                 'provcalldur_mth_avg', 'domecalldur_mth_avg', 'native_orig_cnt', 'nativeorigdur_avg',
                 'nativeorigcnt_mth_avg', 'nativeorigdur_mth_avg', \
                 'native_term_cnt', 'nativetermdur_avg', 'nativetermcnt_mth_avg', 'nativetermdur_mth_avg',
                 'prov_orig_cnt',
                 'provorigdur_avg', \
                 'provorigcnt_mth_avg', 'provorigdur_mth_avg', 'prov_term_cnt', 'provtermdur_avg',
                 'provtermcnt_mth_avg',
                 'provtermdur_mth_avg', \
                 'dome_orig_cnt', 'domeorigdur_avg', 'domeorigcnt_mth_avg', 'domeorigdur_mth_avg', 'dome_term_cnt',
                 'dometermdur_avg', \
                 'dometermcnt_mth_avg', 'dometermdur_mth_avg', 'calldur_mthavg_day', 'calldur_mthavg_night',
                 'calldur_night_rate',
                 'callcnt_mthavg_day', 'callcnt_mthavg_night', \
                 'callcnt_night_rate', 'work_calldur_mthavg', 'nonwork_calldur_mthavg', 'nonwork_calldur_rate',
                 'work_callcnt_mthavg', 'nonwork_callcnt_mthavg', \
                 'nonwork_callcnt_rate', 'workday_calldur_mthavg', 'weekday_calldur_mthavg', 'workday_calldur_rate',
                 'weekday_calldur_rate', 'workday_callcnt_mthavg', \
                 'weekday_callcnt_mthavg', 'workday_callcnt_rate', 'weekday_callcnt_rate', 'callcnt_over20_mths',
                 'calldur_over5min_cnt', 'over5m_2t_callcnt', \
                 'clofri_cnt', 'mostclofri_calldur_mthavg', 'mostclofri_callcnt_mthavg',
                 'mostclofri_calldur_mthavg_day', \
                 'mostclofri_calldur_mthavg_night', 'mostclofri_callcnt_mthavg_day', 'mostclofri_callcnt_mthavg_night',
                 'clofri_calldur_rate', 'clofri_callcnt_rate', 'min_calldur', 'med_calldur', 'spe_calldur',
                 'min_callcnt', \
                 'med_callcnt', 'spe_callcnt', 'insetion_cnt', 'holiday_callcnt', 'holiday_callloc_cnt', \
                 'holiday_calldur_mthavg']

    var = pd.DataFrame([vars_list], index=pd.unique(df.user_number), columns=vars_name)
    var.index.rename('mobile', inplace=True)
    var = var.reset_index()
    var.index = [0]
    return var
