# 加载模型
# 预测结果
# 结果返回
import pickle
import xgboost as xgb
import numpy as np
import os


def get_model_path(model_name):
    file_path = os.path.split(os.path.realpath(__file__))[0]
    # os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
    path = os.path.join(file_path, 'exdata', model_name)
    return path


def load_model(path):
    with open(path, 'rb') as f:
        bst = pickle.load(f)
    return bst


def model_data(test):
    dtest = xgb.DMatrix(test, missing=np.nan)
    return dtest


def predict_score(dtest, bst):
    test_pred = bst.predict(dtest)
    return test_pred


def score_check(raw_score):
    for i in raw_score:
        if i < 500:
            raw_score[raw_score == i] = 501
        elif i > 850:
            raw_score[raw_score == i] = 849
    return raw_score


def score(test_pred):
    score = 660 - 50 / np.log(2) * np.log(test_pred / (1 - test_pred))
    return score_check(np.round(score, 0))


'''
count    43310.000000
mean       610.329468
std         21.130302
min        538.000000
25%        596.000000
50%        612.000000
75%        626.000000
max        668.000000

count    43310.000000
mean       615.493164
std         31.694601
min        507.000000
25%        594.000000
50%        617.000000
75%        639.000000
max        702.000000

count    43310.000000
mean       636.154175
std         73.949890
min        384.000000
25%        586.000000
50%        641.000000
75%        691.000000
max        837.000000

count    43310.000000
mean       691.821167
std         52.817970
min        512.000000
25%        656.000000
50%        695.000000
75%        731.000000
max        836.000000

'''
