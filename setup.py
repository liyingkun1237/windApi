from distutils.core import setup

setup(
    name='windApi',
    version='0.1.0',
    packages=['windApi'],
    url='2017-10-24',
    license='ccx',
    author='liyingkun',
    author_email='liyingkun@ccx.cn',
    description='风云实验室模型',
    package_data={'': ['*.py', 'exdata/prov_city_county_dic.txt',
                       'exdata/base_model_2017-07-27.txt',
                       'exdata/bm_model_2017-07-24.txt'
                       ]},
    data_files=[('', ['setup.py'])]
)
