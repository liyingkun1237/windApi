import pandas as pd

imp_var = pd.read_csv(r'C:\Users\liyin\Desktop\windApi\imp_var.csv')
raw_data = pd.read_csv(r'C:\Users\liyin\Desktop\windApi\rawdata.csv')

import numpy as np
from sklearn import tree
from inspect import getmembers


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
    return pd.DataFrame({"Feature_Name": crtab[['varname']].drop_duplicates().varname.values,
                         "data": [crtab[['var_level', 'factor_per', 'bad_per', 'IV']].to_dict('list')]})


def numeric_var(x, y):
    # 使用决策树分箱的连续变量不能含有缺失，这里使用均值填补
    x.fillna(x.mean(), inplace=True)
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

                    print(data[varname[j]])
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


xx = get_imp_bad(imp_var, raw_data)
xx.to_csv(r'C:\Users\liyin\Desktop\windApi\caogao.csv', index=False)
numeric_var(raw_data.age, raw_data.target).to_json(orient='records')

xx.to_json(orient='records')

IV(raw_data.is_night_apply, raw_data.target)
IV(raw_data.is_night_apply, raw_data.target).to_dict('list')
xx.to_dict()
