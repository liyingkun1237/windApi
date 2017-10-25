'''
依据 cxh base_var 的代码 进行进一步的优化和封装
主要的优化思路：
1.函数细化与细节优化
2.函数扩展性的加强，既能计算同牛现有的base数据，又能计算风云实验室的数据，同时也能满足Rscore分的要求
3.即可以计算单条，又支持批量
4.支持并行计算，优化计算的速度
'''

import pandas as pd
import numpy as np
from datetime import datetime
import re
import os


def f_apply_dts(audit_time, apply_time):
    '''
    时间差 s
    :param audit_time: 信审时间 2016-12-29 13:13:24 日期格式，而非字符串
    :param apply_time: 申请时间 2016-12-29 13:13:24
    :return: 信审时间 - 申请时间 单位 s 秒
    '''
    ls = []
    for i, j in zip(audit_time, apply_time):
        ls.append((i - j) / np.timedelta64(1, 's'))
    return ls


def f_is_night_apply(audit_time):
    '''
    是否夜间申请
    :param audit_time: 信审时间
    :return: 0,1 夜间申请：1 定义 [0-5]
    '''
    x = audit_time.hour
    if x in list(range(6)):
        return 1
    elif x in list(range(6, 24)):
        return 0


def f_is_workday_apply(audit_time):
    '''
    是否是工作日申请 [0-5) 工作日
    :param audit_time:
    :return:
    '''
    x = datetime.weekday(audit_time)
    if x in list(range(5)):
        return 1
    elif x in list(range(5, 7)):
        return 0


def f_is_worktime_apply(audit_time):
    '''
    是否是工作时间申请 9-18
    :param audit_time:
    :return:
    '''
    x = audit_time.hour
    if x in list(range(9, 19)):
        return 1
    else:
        return 0


def f_apply_amount(apply_amount):
    '''
    申请金额 ：直接获取
    :param apply_amount:
    :return:
    '''
    # 如需要 可以加上数据校验逻辑 或者是分箱逻辑
    return apply_amount


def f_apply_month(apply_month):
    '''
    申请的月份数
    :param apply_month:
    :return:
    '''
    return apply_month


def f_register_diffdays(present_time, register_time):
    '''
    注册时间距今的天数
    :param present_time:
    :param register_time:
    :return:
    '''
    ls = []
    for i, j in zip(present_time, register_time):
        ls.append((i - j) / np.timedelta64(1, 'D'))
    return ls


# var9 推广渠道来源
def f_tg_location(tg_location):
    '''
    渠道来源 多类别归约
    :param tg_location:
    :return:
    '''
    tg_location_list1 = ['xjbka', 'yingyongbao', 'wtnsu', 'dwcm', 'rongyitui', 'jieba']
    tg_location_list2 = ['ryt', 'vivo', 'laijieqian', 'xianshangdaikuan', 'tqb', 'anxinj', '51gjj', 'xiaoxinyong',
                         'xianjindai']
    tg_location_list3 = ['yuyongjinrong', 'yijiexianjindai', 'nalijie', 'jiandandaikuan', 'xiaomi', 'dagejietiao',
                         'haoxiangdai', 'anzhi', 'xdzx',
                         'huchilvxing', '_360', 'diyidaikuan', 'xinyxz', 'jieqianguanjia', 'tdk', 'jieqianyong']

    def get_tglocation(x):
        if pd.notnull(x):
            y = (re.match(r'\S+[a-z]|[^a-z]+[0-9]', 'sdzj2')).group()
        else:
            y = np.nan
        return y

    x = get_tglocation(tg_location)
    if x in ['sdzj']:
        return 1
    elif x in ['xygj']:
        return 2
    elif x in tg_location_list1:
        return 3
    elif x in tg_location_list2:
        return 4
    elif x in tg_location_list3:
        return 5
    else:
        return 5


def f_login_origin(login_origin):
    '''
    注册来源
    :param login_origin:
    :return:
    '''
    return login_origin


def f_is_reapply(is_reapply):
    return is_reapply


def f_address_length(address_length):
    return address_length


def f_mobile_company(mobile_company):
    if mobile_company not in ["2", "1", "3", 1, 2, 3]:
        return 4
    else:
        return mobile_company


def f_mobile_segment(mobile):
    '''
    手机号码前两位
    :param mobile: 标准的11位的号码 如果是含区号或者是+86等信息的数据，需要注意清洗一下
    :return:
    '''
    # 数据清洗 应由前端控制
    try:
        x = str(mobile)[:2]
        if x in ['13', '14', '15', '17', '18']:
            if x == '13':
                return 1
            elif x == '14':
                return 2
            elif x == '15':
                return 3
            elif x == '17':
                return 4
            elif x == '18':
                return 5
        else:
            return np.nan
    except:
        return np.nan


# var 10/11/15/18/20/21/22/23/26
def f_net_age(net_age):
    if net_age in [None, np.nan, '未知']:
        return np.nan
    else:
        return net_age


def f_age(present_time, idno):
    ls = []
    for i, j in zip(present_time, idno):
        try:
            ls.append(i.year - int(j[6:10]))
        except:
            ls.append(np.nan)
    return ls


def f_gender(idno):
    '''
    依据身份证计算出性别 1：男  0：女
    :param idno:
    :return:
    '''
    try:
        return int(idno[-2]) % 2
    except:
        return np.nan


def f_regaud_diffdays(audit_time, register_time):
    '''
    audit_time - register_time 信审时间 减去 注册时间
    :param audit_time:
    :param register_time:
    :return:
    '''
    ls = []
    for i, j in zip(audit_time, register_time):
        ls.append((i - j) / np.timedelta64(1, 's'))
    return ls


# var13/14/16/17/24/25/28/29
def get_prov(x):
    '''
    获取输入字符串的省份关键字
    :param x:
    :return:
    '''
    if pd.notnull(x):
        m = re.match(r'\D+(省|自治区|特别行政区)', x)
        if m:
            y = re.split('省|自治区|特别行政区', m.group(0))[0]
        else:
            mm = re.match(r'\D+市', x)
            if mm:
                y = re.split('市', mm.group(0))[0]
            else:
                y = x
    else:
        y = np.nan
    return y


def f_dict_prov_level(x):
    pattern_1 = '上海|北京|天津|广东|澳门|香港'
    pattern_2 = '江苏|山东|福建|浙江|安徽'
    pattern_3 = '湖北|湖南|河南|河北|江西|山西|陕西'
    pattern_4 = '广西|海南|云南|四川|贵州|重庆'
    pattern_5 = '黑龙江|吉林|辽宁|内蒙古|甘肃|新疆|青海|宁夏|西藏'
    if re.search(pattern_1, str(x)) is not None:
        return 1
    elif re.search(pattern_2, str(x)) is not None:
        return 2
    elif re.search(pattern_3, str(x)) is not None:
        return 3
    elif re.search(pattern_4, str(x)) is not None:
        return 4
    elif re.search(pattern_5, str(x)) is not None:
        return 5
    else:
        return 6


def f_cid_prov_lvl(id_card_address):
    '''
    获取身份证的省份等级
    :param id_card_address:
    :return:
    '''
    return f_dict_prov_level(get_prov(id_card_address))


def f_addr_prov_lvl(user_province):
    '''
    获取现居住地的省份等级
    :param user_province:
    :return:
    '''
    return f_dict_prov_level(get_prov(user_province))


def f_mobile_prov_lvl(mobile_province):
    '''
    获取手机号归属地的省份等级
    :param mobile_province:
    :return:
    '''
    return f_dict_prov_level(get_prov(mobile_province))


def f_dict_city_level(x):
    pattern_1 = '北京|上海|广州|深圳|天津'  # 一线
    pattern_2 = '杭州|南京|济南|重庆|青岛|大连|宁波|厦门'  # 二线发达
    pattern_3 = '成都|武汉|哈尔滨|沈阳|西安|长春|长沙|福州|郑州|石家庄|苏州|佛山|东莞|无锡|烟台|太原'  # 二线中等发达
    pattern_4 = '合肥|南昌|南宁|昆明|温州|淄博|唐山'  # 二线发展较弱

    if x not in [None, np.nan]:
        if re.search(pattern_1, str(x)) is not None:
            return 1
        elif re.search(pattern_2, str(x)) is not None:
            return 2
        elif re.search(pattern_3, str(x)) is not None:
            return 3
        elif re.search(pattern_4, str(x)) is not None:
            return 4
        else:
            return 5  # 三线以下城市
    else:
        return 6  # 缺失


def get_city(x):
    '''
    获取给定地址字符串中的市
    :param x:
    :return:
    '''
    if pd.notnull(x):
        m = re.match(r'\D+(省|自治区)\D*?市', x)
        if m:
            yy = re.split('省|自治区', m.group(0))[1]
            m1 = re.match(r'\D+地区\D+市', x)
            if m1:
                y = re.split('地区', m1.group())[1]
            else:
                y = yy
        else:
            mm = re.match(r'\D*?市', x)
            if mm:
                y = mm.group(0)
            else:
                mmm = re.match(r'\D+地区\D+县', x)
                if mmm:
                    y = re.split('地区', mmm.group())[1]
                else:
                    y = np.nan
    else:
        y = np.nan
    return (y)


file_path = os.path.split(os.path.realpath(__file__))[0]
path = os.path.join(file_path, 'exdata', 'prov_city_county_dic.txt')
addr_dic = pd.read_csv(path, sep='\t', encoding='gbk')


# 读取省市地区对应表
# 保证读取目录的存在性
# os.chdir(os.path.split(os.path.realpath(__file__))[0])
# addr_dic = pd.read_csv(r'./exdata/prov_city_county_dic.txt', sep='\t', encoding='gbk')


def county_match_city(x):
    '''
    通过省 、 市 的外部数据字典 进行城市的匹配
    :param x: (cid_prov,cid_city)
    :return:
    '''

    if pd.notnull(x[1]):
        m = re.match(r'\D+市', x[1])
        if m:
            y = x[1]
        else:
            try:
                y = addr_dic.ix[(addr_dic['county'] == x[1]) & (addr_dic['prov'] == x[0]), 'city'].tolist()[0]
            except:
                y = 'unknown'
    else:
        y = np.nan
    return (y)


def f_cid_city_lvl(id_card_address):
    '''
    身份证地址信息 获取城市，并归约于对应的等级
    :param id_card_address:
    :return:
    '''
    x = [get_prov(id_card_address), get_city(id_card_address)]
    city = county_match_city(x)
    return f_dict_city_level(city)


def f_addr_city_lvl(user_city):
    '''
    居住地 获取城市，并归约于对应的等级
    :param user_city:
    :return:
    '''
    # x = [get_prov(user_city),get_city(user_city)]
    # city = county_match_city(user_city)
    return f_dict_city_level(user_city)


def f_mobile_city_lvl(mobile_city):
    '''
    手机号归属地 获取城市，并归约于对应的等级
    :param mobile_city:
    :return:
    '''
    # x = [get_prov(user_city),get_city(user_city)]
    # city = county_match_city(user_city)
    return f_dict_city_level(mobile_city)


def f_bank_prov_lvl(bank_address):
    '''
    依据所给地址信息 匹配出省份的等级
    :param bank_address:
    :return:
    '''
    return f_dict_prov_level(bank_address)


def f_bank_city_lvl(bank_address):
    return f_dict_city_level(bank_address)


# var30/31/33/34
def f_job_type(job):
    if job in ['公司受雇员工', '私企公司职工']:
        return 1
    elif job in ['国企事业单位职工', '公务员', '农林牧副渔人员', '军人']:
        return 2
    elif job in ['自雇创业人员', '自主创业人员']:
        return 3
    elif job in ['自由职业者', '家庭主妇', '失业人员', '退休人员']:
        return 4
    else:
        return 1


def f_rela1_lvl(relation1):
    if str(relation1).strip() == '父亲':
        return 1
    elif str(relation1).strip() == '母亲':
        return 2
    elif str(relation1).strip() == '配偶':
        return 3
    elif str(relation1).strip() == '子女':
        return 4
    else:
        return 5


def f_rela2_lvl(relation2):
    if str(relation2).strip() == '其他亲属':
        return 1
    elif str(relation2).strip() in ['兄弟', '姐妹']:
        return 2
    elif str(relation2).strip() == '配偶':
        return 3
    elif str(relation2).strip() in ['母亲', '父亲']:
        return 4
    else:
        return 5


def f_rela3_lvl(relation3):
    if str(relation3).strip() == '朋友':
        return 1
    elif str(relation3).strip() == '同事':
        return 2
    elif str(relation3).strip() in ['其他', '其他亲属']:
        return 3
    else:
        return 4


# var35/../39/41
def f_2equal_addr(x, y):
    if x not in [None, np.nan] and y not in [None, np.nan]:
        x = str(x)
        y = str(y)
        if len(x) > len(y):
            return re.search(y, x) is not None
        elif len(x) <= len(y):
            return re.search(x, y) is not None
        else:
            return False
    else:
        return False


def f_2compare_addr(x, y):
    ls = []
    for i, j in zip(x, y):
        if f_2equal_addr(i, j):
            ls.append(1)
        else:
            ls.append(0)
    return ls


def f_cidcity_bankcity(id_card_address, bank_address):
    return f_2compare_addr(id_card_address, bank_address)


def f_addrcity_bankcity(user_city, bank_city):
    return f_2compare_addr(user_city, bank_city)


def f_addrcity_mobilecity(user_city, mobile_city):
    return f_2compare_addr(user_city, mobile_city)


def f_mobilecity_compcity(mobile_city, company_city):
    return f_2compare_addr(mobile_city, company_city)


def f_addrcity_compcity(user_city, company_city):
    return f_2compare_addr(user_city, company_city)


def f_mobile_cardmobile(mobile, card_mobile):
    ls = []
    for i, j in zip(mobile, card_mobile):
        if i == j:
            ls.append(1)
        else:
            ls.append(0)
    return ls


def f_getdummy(basevar):
    dummyvar = ['tg_location', 'login_origin', 'mobile_company', 'mobile_segment', 'cid_prov_lvl', 'addr_prov_lvl',
                'mobile_prov_lvl', 'cid_city_lvl', 'addr_city_lvl', 'bank_prov_lvl', 'bank_city_lvl', 'mobile_city_lvl',
                'job_type', 'rela1_lvl', 'rela2_lvl', 'rela3_lvl']

    dummycnt = [4, 2, 3, 4, 5, 5, 5, 5, 5, 5, 5, 5, 3, 4, 4, 3]

    for i in np.arange(16):
        for j in np.arange(dummycnt[i] + 1):
            if list(basevar[dummyvar[i]].values)[0] == j:
                basevar[dummyvar[i] + '_' + str(j + 1)] = 1
            else:
                basevar[dummyvar[i] + '_' + str(j + 1)] = 0
        del basevar[dummyvar[i]]

    return basevar


def tran_date(x):
    try:
        return pd.to_datetime(x)
    except:
        return pd.to_datetime('')  # 返回时间形式的空 便于后面时间做减法不能正常进行


'''
开始组装函数 并最终计算出变量
'''


def get_basevars(df, present_time, detail=False):
    # 1.数据的类型转换（时间类型的数据）
    df['application_uuid'] = df['application_uuid']

    df['audit_time'] = df['audit_time'].apply(tran_date)
    df['apply_time'] = df['apply_time'].apply(tran_date)
    df['register_time'] = df['register_time'].apply(tran_date)
    df['present_time'] = present_time

    basevar = df.assign(
        apply_dts=lambda x: f_apply_dts(x.audit_time, x.apply_time),
        is_night_apply=lambda x: x.audit_time.apply(f_is_night_apply),

        is_workday_apply=lambda x: x.audit_time.apply(f_is_workday_apply),
        is_worktime_apply=lambda x: x.audit_time.apply(f_is_worktime_apply),
        apply_amount=lambda x: x.apply_amount.apply(f_apply_amount),
        apply_month=lambda x: x.apply_month.apply(f_apply_month),
        register_diffdays=lambda x: f_register_diffdays(x.present_time, x.register_time),
        tg_location=lambda x: x.tg_location.apply(f_tg_location),
        login_origin=lambda x: x.login_origin.apply(f_login_origin),
        is_reapply=lambda x: x.is_reapply.apply(f_is_reapply),
        address_length=lambda x: x.address_length.apply(f_address_length),
        mobile_company=lambda x: x.mobile_company.apply(f_mobile_company),
        mobile_segment=lambda x: x.mobile.apply(f_mobile_segment),
        net_age=lambda x: x.net_age.apply(f_net_age),
        age=lambda x: f_age(x.present_time, x.idno),
        gender=lambda x: x.idno.apply(f_gender),
        regaud_diffdays=lambda x: f_regaud_diffdays(x.audit_time, x.register_time),
        cid_prov_lvl=lambda x: x.id_card_address.apply(f_cid_prov_lvl),
        addr_prov_lvl=lambda x: x.user_province.apply(f_addr_prov_lvl),
        mobile_prov_lvl=lambda x: x.mobile_province.apply(f_mobile_prov_lvl),
        cid_city_lvl=lambda x: x.id_card_address.apply(f_cid_city_lvl),
        addr_city_lvl=lambda x: x.user_city.apply(f_addr_city_lvl),
        bank_prov_lvl=lambda x: x.bank_address.apply(f_bank_prov_lvl),
        bank_city_lvl=lambda x: x.bank_address.apply(f_bank_city_lvl),
        mobile_city_lvl=lambda x: x.mobile_city.apply(f_mobile_city_lvl),
        job_type=lambda x: x.job.apply(f_job_type),
        rela1_lvl=lambda x: x.relation1.apply(f_rela1_lvl),
        rela2_lvl=lambda x: x.relation2.apply(f_rela2_lvl),
        rela3_lvl=lambda x: x.relation3.apply(f_rela3_lvl),
        cidcity_bankcity=lambda x: f_cidcity_bankcity(x.id_card_address.apply(get_city), x.bank_address),
        addrcity_bankcity=lambda x: f_addrcity_bankcity(x.user_city, x.bank_address),
        mobile_cardmobile=lambda x: f_mobile_cardmobile(x.mobile, x.card_mobile),
        addrcity_mobilecity=lambda x: f_addrcity_mobilecity(x.user_city, x.mobile_city),
        addrcity_compcity=lambda x: f_addrcity_compcity(x.user_city, x.company_city),
        mobilecity_compcity=lambda x: f_mobilecity_compcity(x.mobile_city, x.company_city),

    )

    select_col = ['application_uuid', 'apply_dts', 'is_night_apply', 'is_workday_apply', 'is_worktime_apply',
                  'apply_amount', 'apply_month', 'register_diffdays', 'tg_location', 'login_origin', 'is_reapply',
                  'address_length', 'mobile_company', 'mobile_segment', 'net_age', 'age', 'gender', 'regaud_diffdays',
                  'cid_prov_lvl', 'addr_prov_lvl', 'mobile_prov_lvl', 'cid_city_lvl', 'addr_city_lvl', 'bank_prov_lvl',
                  'bank_city_lvl', 'mobile_city_lvl', 'job_type', 'rela1_lvl', 'rela2_lvl', 'rela3_lvl',
                  'cidcity_bankcity', 'addrcity_bankcity', 'mobile_cardmobile', 'addrcity_mobilecity',
                  'addrcity_compcity', 'mobilecity_compcity']

    basevar = basevar[select_col]

    if 'tid' in df.columns:  # 主要为风云实验室需要
        basevar['target'] = df['target'].astype(int)
        basevar['tid'] = df['tid']

    basevar1 = basevar.copy()

    if 'mobile' in df.columns:
        basevar['mobile'] = df['mobile']  # 1023为了方便和通话详单匹配数据

    basevar = f_getdummy(basevar)

    if detail:
        return basevar, basevar1  # cxh
    else:
        return basevar


if __name__ == '__main__':
    df = pd.read_csv(r'C:\Users\Administrator\Desktop\tn_0713\base_rawdata_0717.csv', encoding='gbk')
    df.idno
    import time

    start = time.time()
    xx = get_basevars(df.head(10000), present_time=pd.to_datetime('2017-06-11'), detail=True)
    print(time.time() - start)
