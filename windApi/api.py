'''
实现的功能，接受请求，并获取数据
'''
import datetime
import time
import flask
from flask import jsonify
from flask import request

import json
from windApi.get_basevar import get_basevars
import pandas as pd
from windApi.model import load_model, model_data, predict_score, score, get_model_path
from sklearn.metrics import roc_curve, auc
import threading
import requests

from windApi.mobile_var import get_mobilevars

'''
post请求，请求参数入参类型json
{name:{"key":"value"}}
{"id":"data"}
'''

server = flask.Flask(__name__)


@server.route('/get_data', methods=['post'])
def get_data():
    try:
        # 获取数据
        payload = json.loads(request.data.decode())
        print('请求数据====：：：', payload)
        model_code = payload.get("model_code")
        y_code = payload.get("y_code")
        tid = payload.get("reqid")
        data = payload.get("data")
        # print(data)
        # json --> DataFrame --> computer variable
        base_data = pd.DataFrame(data)
        curDate = datetime.date.today().strftime('%Y-%m-%d')
        start = time.time()
        var, rawdata = get_basevars(base_data.head(1), pd.to_datetime(curDate), detail=True)
        print('计算时间', time.time() - start)
        # print(var)
        var.index = var.application_uuid
        var.drop(['application_uuid', 'target'], axis=1, inplace=True)
        var = var.astype('float')
        # 起个线程 计算只有申请资料的base模型
        if model_code == '1':
            with server.app_context():
                t = threading.Thread(target=my_computer_thread, args=(base_data, curDate, tid))
                t.start()
                # t.join() 同步和异步的区别
        elif model_code == '2':
            with server.app_context():
                t = threading.Thread(target=bm_computer_thread, args=(base_data, curDate, tid))
                t.start()

        return jsonify({"code": 200, "msg": '调用成功'})
    except Exception as e:
        return jsonify({"code": 500, "msg": str(e)})


def my_computer_thread(base_data, curDate, tid):
    print('运行到这')
    # base_data.to_csv('raw_data.csv', index=False)
    # print(np.sum(pd.isnull(base_data)))
    # print(len(base_data))
    var, rawdata = get_basevars(base_data, pd.to_datetime(curDate), detail=True)
    # rawdata.to_csv(r'C:\Users\liyin\Desktop\windApi\rawdata.csv', index=False)
    var.index = var.application_uuid
    var.drop(['application_uuid', 'target', 'tid', 'mobile'], axis=1, inplace=True)
    var = var.astype('float')

    # 3.加载模型
    model_path = get_model_path('base_model_2017-07-27.txt')
    # print('model_path:%s' % model_path)
    bst = load_model(model_path)
    imp_var = get_importance_var(bst)
    # imp_var.to_csv(r'C:\Users\liyin\Desktop\windApi\imp_var.csv', index=False)
    impvar = get_imp_bad(imp_var, rawdata).to_json(orient='records', force_ascii=False)
    # print(impvar)
    p_value = predict_score(model_data(var), bst)
    pre_score = score(p_value)

    re = pd.DataFrame({'reTid': tid,
                       'application_ID': var.index,
                       'P_value': p_value,
                       'application_score': pre_score,
                       'runtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                       }, index=None)
    # .to_json(orient='records', force_ascii=False)

    fpr, tpr, _ = roc_curve(rawdata.target, re.P_value)
    roc = [list(fpr), list(tpr)]
    score_ = [int(x) for x in pre_score]
    # impvar = {"importVar": [{"Feature_Name": "apply_dts", "data": [[20.5, 30, 40, 9.5], [10, 35, 45, 10]]}]}

    re_data = {"code": 200, "reqid": tid, "data": {"roc": roc, "socre": score_, "plot_impvar": impvar}}
    # print(re_data)
    # 计算完毕，调用结果返回接口，将数据传回前端
    # url = 'http://192.168.100.164:8086/ccx_antifraud_web/lab/rcModelData'
    url = 'http://192.168.100.34:8085/ccx_antifraud_web/lab/rcModelData'
    # print(json.dumps(re_data, ensure_ascii=False))
    r = requests.post(url, data=json.dumps(re_data))
    # print(r.encoding)
    print('模型输出数据：', json.dumps(re_data))
    print('回调结果', r.text)
    print('计算线程运行结束')


def bm_computer_thread(base_data, curDate, tid):
    var, rawdata = get_basevars(base_data, pd.to_datetime(curDate), detail=True)
    # rawdata.to_csv(r'C:\Users\liyin\Desktop\windApi\rawdata.csv', index=False)
    var.index = var.mobile

    # 去请求数据平台的通话详单数据
    url_1 = r'http://192.168.100.111:9085/data-service/operator/simple/data/batchquery?'
    account = 'wxfy-neibu-api'
    reqTid = str(tid)
    sign_str = 'account' + account + 'reqTid' + reqTid + '94a17533217e6893'
    # print(sign_str)
    sign = mob2MD5(sign_str).upper()
    j = ''
    for i in var.tid.values:
        j += str(i) + ','
    tids = r'&sessionids={}'.format(j[:-1])

    URL = url_1 + 'account=' + account + '&reqTid=' + reqTid + '&sign=' + sign + tids
    # print('cscc URL:', URL)
    url = r'http://192.168.100.111:9085/data-service/operator/simple/data/batchquery?account=wxfy-neibu-api&sign=5A3860AEB455D5D1F53D8A8CCB2FC01C&reqTid=1234&sessionids=09E90A07CE304624B9CBAFC67A7FDBDA'
    r = requests.post(url)
    Need_col = ["mobile", "transmitType", "duration", "startTime", "location", "otherNum", "callType", "inqueryTime"]
    M_data = pd.DataFrame(json.loads(r.text)['data'])[Need_col]
    # 对数据进行预处理
    M_data['duration'] = M_data.duration.apply(getTime)
    M_data['callType'] = M_data.callType.apply(trans_callChannel)
    M_data['startTime'] = pd.to_datetime(M_data.startTime)
    M_data['inqueryTime'] = pd.to_datetime(M_data.inqueryTime)  # 查询时间的正确理解
    Name_dict = {"mobile": "user_number", "transmitType": "land_type", "duration": "times",
                 "startTime": "stat_time", "location": "call_address", "otherNum": "other_number",
                 "callType": "call_channel", "inqueryTime": "inquiry_time"}
    M_data.rename(columns=Name_dict, inplace=True)
    M_var = f_count_allmobile_var(M_data)

    VAR = pd.merge(var, M_var, left_index=True, right_index=True, how='left')

    # 对通话详单的变量进行重命名，后期更新模型之后不存在这个问题
    rename_dict = {'latestcall_diffdavs': 'latestcall_diffdays',
                   'provorigcdur_mth_avg': 'provorigdur_mth_avg',
                   'holiday_callcnt_mthavg': 'holiday_callcnt',
                   'provtermcdur_mth_avg': 'provtermdur_mth_avg',
                   'nativetermcdur_mth_avg': 'nativetermdur_mth_avg',
                   'dometermcdur_mth_avg': 'dometermdur_mth_avg',
                   'domcalldur_avg': 'domecalldur_avg',
                   'domeorigcdur_mth_avg': 'domeorigdur_mth_avg',
                   'termcallcnt_mth_min': 'termcallcnt_mth_max',
                   'origcallcnt_mth_min': 'origcallcnt_mth_max',
                   }

    rename_dict_ = {}
    for i in rename_dict.keys():
        rename_dict_[rename_dict[i]] = i

    VAR.rename(columns=rename_dict_, inplace=True)
    VAR['tg_location_6'] = np.nan
    VAR['callnum_trend_mth'] = np.nan

    # 3.加载模型
    model_path = get_model_path('bm_model_2017-07-24.txt')
    # r'C:\Users\liyin\Desktop\code_share\share_datamodel\lyk\model_2017-07-24.txt'
    # print('model_path:%s' % model_path)
    bst = load_model(model_path)
    imp_var = get_importance_var(bst)
    # imp_var.to_csv(r'C:\Users\liyin\Desktop\windApi\imp_var.csv', index=False)
    impvar = get_imp_bad(imp_var, rawdata).to_json(orient='records', force_ascii=False)  # 批注：重要变量为申请资料+通话详单计算出的变量
    # print(impvar)
    VAR = VAR[bst.feature_names].astype('float')
    # print(VAR.columns)
    p_value = predict_score(model_data(VAR[bst.feature_names]), bst)
    pre_score = score(p_value)

    re = pd.DataFrame({'reTid': tid,
                       'application_ID': VAR.index,
                       'P_value': p_value,
                       'application_score': pre_score,
                       'runtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                       }, index=None)
    # .to_json(orient='records', force_ascii=False)

    fpr, tpr, _ = roc_curve(rawdata.target, re.P_value)
    ks = np.round(np.max(tpr - fpr) * 100, 2)
    Auc = np.round(auc(fpr, tpr) * 100, 2)
    roc = [list(fpr), list(tpr), {'ks': ks}, {'Auc': Auc}]
    print('roc===', {"roc": roc})
    score_ = [int(x) for x in pre_score]
    # impvar = {"importVar": [{"Feature_Name": "apply_dts", "data": [[20.5, 30, 40, 9.5], [10, 35, 45, 10]]}]}

    re_data = {"code": 200, "reqid": tid, "data": {"roc": roc, "socre": score_, "plot_impvar": impvar}}
    # print(re_data)
    # 计算完毕，调用结果返回接口，将数据传回前端
    url = 'http://192.168.100.34:8085/ccx_antifraud_web/lab/rcModelData'
    # print(json.dumps(re_data, ensure_ascii=False))
    r = requests.post(url, data=json.dumps(re_data))
    print('模型输出数据：', json.dumps(re_data))
    print('回调结果', r.text)
    print('计算线程运行结束--mobile ')


import re


def f_count_allmobile_var(data_):
    ls = []
    i = 1
    mobile_list = data_.user_number.unique()
    n = len(mobile_list)
    for mobile in mobile_list:
        x = data_[data_.user_number == mobile]
        ls.append(get_mobilevars(x))
        i += 1
        if i % 100 == 0:
            print('运行到第%s 行，总计运行%s 行' % (i, n))
    return pd.concat(ls)


def getTime(str):
    re_minute = re.compile(r'\d{1,2}分\d{1,2}秒')
    re_second = re.compile(r'\d{1,2}秒')
    if (re_minute.match(str) != None):
        return float(str.split('分')[0]) * 60 + float(str.split('分')[1][0:-1])
    elif (re_second.match(str) != None):
        return float(str[0:-1])
    else:
        # 没有匹配成功 加入日志信息
        return -0.5


def trans_callChannel(x):
    if str(x).strip() == '主叫':
        return 1
    elif str(x).strip() == '被叫':
        return 0
    else:
        return np.nan


# x = '12分28秒'
# y = '26秒'
#
# print(getTime(x))

import hashlib


def mob2MD5(string):
    m = hashlib.md5()
    mob = string.encode('utf-8')
    m.update(mob)
    psw = m.hexdigest()
    return psw


def get_importance_var(bst):
    '''
    获取进入模型的重要变量
    '''
    re = pd.Series(bst.get_score(importance_type='gain')).sort_values(ascending=False)
    re = pd.DataFrame(re, columns=['gain']).reset_index()
    re.columns = ['Feature_Name', 'gain']
    re = re.assign(
        pct_importance=lambda x: x['gain'].apply(lambda s: str(np.round(s / np.sum(x['gain']) * 100, 2)) + '%'))
    # print('重要变量的个数：%d' % len(re))
    return re


import numpy as np
from sklearn import tree
from inspect import getmembers

dict_name = {'apply_dts': '申请时长',
             'is_night_apply': '是否夜间申请',
             'is_worktime_apply': '是否工作时间申请',
             'is_workday_apply': '是否工作日申请',
             'is_holiday_apply': '是否节假日申请',
             'apply_amount': '申请金额',
             'apply_month': '申请期限',
             'register_diffdays': '注册时间距今天数',
             'tg_location': '推广渠道来源',
             'login_origin': '注册来源',
             'is_reapply': '是否复借',
             'reapply_cnt': '复借次数',
             'cid_prov_lvl': '身份证归属省份等级',
             'cid_city_lvl': '身份证归属城市等级',
             'address_length': '地址长度',
             'bank_prov_lvl': '银行卡开户地归属省份等级',
             'bank_city_lvl': '银行卡开户地归属城市等级',
             'mobile_company': '运营商类型',
             'mobile_segment': '号段',
             'mobile_segment_2num': '号段前两位',
             'net_age': '入网时长',
             'age': '年龄',
             'gender': '性别',
             'addr_prov_lvl': '居住省份等级',
             'addr_city_lvl': '居住城市等级',
             'regaud_diffdays': '注册时间到进件时间的差值',
             'login_diffdays': '最近一次登录时间距今天数',
             'mobile_prov_lvl': '注册手机入网省份',
             'mobile_city_lvl': '注册手机入网城市等级',
             'job_type': '职业',
             'rela1_lvl': '联系人1关系类型',
             'r_surname': '直系与本人姓氏是否一致',
             'rela2_lvl': '联系人2关系类型',
             'rela3_lvl': '联系人3关系类型',
             'cidcity_bankcity': '身份证归属地与银行卡开户地是否一致',
             'addrcity_bankcity': '居住城市与银行卡开户地是否一致',
             'mobile_cardmobile': '注册手机号与银行卡预留手机号是否一致',
             'addrcity_mobilecity': '居住城市与手机归属地是否一致',
             'addrcity_compcity': '居住城市与公司所在城市是否一致',
             'comcity_londis_code': '公司区号和公司所在城市是否一致',
             'mobilecity_compcity': '公司所在城市与手机归属城市是否一致',
             'mobile': '申请人手机号'
             }


def IV(x, y):
    crtab = pd.crosstab(x, y, margins=True)
    crtab.columns = ['good', 'bad', 'total']
    crtab['factor_per'] = np.round(crtab['total'] * 100 / len(y), 2)
    crtab['bad_per'] = np.round(crtab['bad'] * 100 / crtab['total'], 2)
    crtab['p'] = crtab['bad'] / crtab.ix['All', 'bad']
    crtab['q'] = crtab['good'] / crtab.ix['All', 'good']
    crtab['woe'] = np.log(crtab['p'] / crtab['q'])
    crtab['IV'] = sum((crtab['p'] - crtab['q']) * np.log(crtab['p'] / crtab['q']))
    crtab.drop('All', inplace=True)
    crtab.reset_index(inplace=True)
    crtab['varname'] = crtab.columns[0]
    crtab.rename(columns={crtab.columns[0]: 'var_level'}, inplace=True)
    crtab.var_level = crtab.var_level.apply(str)
    return pd.DataFrame({"Feature_Name": dict_name[crtab[['varname']].drop_duplicates().varname.values[0]],
                         "data": [crtab[['var_level', 'factor_per', 'bad_per', 'IV']].to_dict('list')]})


def numeric_var(x, y):
    # 使用决策树分箱的连续变量不能含有缺失，这里使用均值填补
    try:
        x = x.astype(float)
        x.fillna(x.mean(), inplace=True)
    except:
        pass
    else:
        try:
            ################################方法二 分位点分箱
            x_cut = pd.qcut(x, 5)
            oput = IV(x_cut, y)
        except:
            ###############################方法一 决策树分箱
            vary = y.tolist()
            varx = []
            xlist = x.tolist()
            for i in range(len(xlist)):
                varx.append([xlist[i]])

            clf = tree.DecisionTreeClassifier(max_leaf_nodes=8, min_samples_leaf=0.05)
            model = clf.fit(varx, vary)
            # 存为dot文件，保存在当前目录下
            # with open("applyamt.dot",'w') as f:f=tree.export_graphviz(applyamt_tree,out_file=f,feature_names='apply_amount')
            v_tree = getmembers(model.tree_)
            v_tree_thres = dict(v_tree)['threshold']
            v_tree_thres = sorted(list(v_tree_thres[v_tree_thres != -2]))
            split_p = [min(x)] + v_tree_thres + [max(x)]
            x_cut = pd.cut(x, split_p, right=True)
            oput = IV(x_cut, y)

    return oput


# impvar是Dataframe,保存模型重要变量，data是未one-hot编码前的数据
def get_imp_bad(impvar, data):
    impname = impvar['Feature_Name'].tolist()
    # lyk add 0908 主要为了对付，全为空或值唯一的列
    data.dropna(how='all', axis=1, inplace=True)
    varname = data.columns

    impnameorig = []
    imp_bad_result = pd.DataFrame()

    for i in range(len(impname)):
        for j in range(len(varname)):
            namelen = len(varname[j])

            if (impname[i] == varname[j]):
                if len(set(data[varname[j]])) > 10:
                    # print(varname[j])
                    imp_bad_result = pd.concat([imp_bad_result, numeric_var(data[impname[i]], data['target'])])
                    impnameorig.append(impname[i])
                else:
                    imp_bad_result = pd.concat([imp_bad_result, IV(data[impname[i]], data['target'])])
                    impnameorig.append(impname[i])

            elif (impname[i][:namelen] == varname[j]) & (varname[j] not in impnameorig):
                imp_bad_result = pd.concat([imp_bad_result, IV(data[varname[j]], data['target'])])
                impnameorig.append(varname[j])

    return imp_bad_result


if __name__ == '__main__':
    # port可以指定端口，默认端口是5000
    # host默认是127.0.0.1,写成0.0.0.0的话，其他人可以访问，代表监听多块网卡上面，
    # server.run(debug=True, port=8899, host='0.0.0.0')
    server.run(debug=True, port=7060, host='0.0.0.0')
