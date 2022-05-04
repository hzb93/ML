# -*- coding: utf-8 -*-

#修改内容
'''
编写框架 --2020/11/26
'''

#变量
'''
原始数据集
训练集
验证集
测试集
'''


####################################################################################################
# 1 导库
import os
import datetime
import sys
import pyodbc #数据库连接
import pyspark
import traceback
import logging
import scikitplot as skplt
import matplotlib.pyplot as plt
import scipy
import numpy as np
import pandas as pd
import imblearn
import xgboost as xgb
import seaborn as sns
import openpyxl
import lightgbm as lgbm
import joblib
import pymysql #mysql
import warnings #警告
from sklearn.feature_selection import *
from sklearn.preprocessing import * #数据预处理
from imblearn.over_sampling import SMOTE,SMOTENC
from sklearn.model_selection import train_test_split #数据切分
from sklearn.model_selection import StratifiedShuffleSplit #分层采样
from sklearn.linear_model import LogisticRegression
from pylab import *
from IPython.core.interactiveshell import InteractiveShell #系统设置
from dateutil.relativedelta import relativedelta
from pandas.api.types import is_object_dtype
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from category_encoders import *
from sklearn.metrics import *
from imblearn.under_sampling import RandomUnderSampler
from sklearn.svm import SVC
from sklearn.impute import SimpleImputer #空值填补
from sklearn.model_selection import learning_curve #学习曲线
from sklearn.model_selection import KFold, cross_val_score,cross_val_predict
from xgboost import XGBRegressor as XGBR
from category_encoders import * #编码
import re
import cpca #从地址获取行政区域
import missingno as msno #缺失值可视化
import scipy.stats as st #数据分布
import pandas_profiling #可视化数据报告
import cx_Oracle #连接Oracle
from impala.dbapi import connect #连接Impala
from impala.util import as_pandas #Impala转df
import configparser #读取配置文件
from statsmodels.tsa.arima_model import ARIMA #arima
from pmdarima.arima import auto_arima #自动arima
from statsmodels.tsa.holtwinters import ExponentialSmoothing #指数平滑
import itertools
import statsmodels.api as sm


####################################################################################################
# 读取配置文件
conf = configparser.ConfigParser()
conf.read("{}/config/config.ini".format(os.getcwd()))
mysql_info = eval(conf.get("sql_info", "mysqlinfo_dict"))
hive_info = eval(conf.get("sql_info", "hiveinfo_dict"))
impala_info = eval(conf.get("sql_info", "impalainfo_dict"))



####################################################################################################


# 2 系统设置
'''
%matplotlib inline
InteractiveShell.ast_node_interactivity = "all"
plt.rcParams['font.sans-serif'] = ['SimHei']    # 定义使其正常显示中文字体黑体
plt.rcParams['axes.unicode_minus'] = False      # 用来正常显示表示负号
pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns',None)
pd.set_option('display.float_format',lambda x : '%.2f' % x) #禁用科学计数法
warnings.filterwarnings("ignore")
'''

####################################################################################################
# 3 导入数据
# 3.1 Hive
logging.basicConfig(filename='/data11/dataFTP/dmp_net/log/plan_custReduce_GD_ZS_SFXYZS_log.log')
conn = pyodbc.connect('DSN=Cloudera_Hive', autocommit=True)

def hive(dmon):
    hive_sql = """
    select * from dmp_net.caifu_power_new_{0}
    """.format(dmon)

    hive_sqls = hive_sql.split(';')
    for exec_sql in hive_sqls[0:-1]:
        conn = pyodbc.connect('DSN=Cloudera_Hive', autocommit=True)
        Start_time = datetime.datetime.now()
        print(exec_sql)
        print('Start_time:', Start_time)
        conn.execute(exec_sql)
        End_time = datetime.datetime.now()
        print('End_time:', datetime.datetime.now())
        logging.info(exec_sql)
        logging.info('开始执行:' + Start_time.strftime('%Y-%m-%d %H:%M:%S'))
        logging.info('结束执行:' + End_time.strftime('%Y-%m-%d %H:%M:%S'))
        logging.info('本次执行时长为：' + \
            str((End_time - Start_time).seconds) + 's\n')
        conn.close()

engine=pyodbc.connect('DSN=Cloudera_Hive',autocommit=True)
hive_sql="""
select * from dmp_net.caifu_power_new;
"""

data = pd.read_sql(sql=hive_sql,con=engine)



ip = '10.88.20.106'
port = 10099
user = 'gacmdp'
password = 'CU.izAi4'   #生产系统
db = "ods_g063_grt_all_db"
auth = "LDAP"

def hive_to_df(sql):
    """
    功能：将hive数据转换dataframe
    注意：sql不需要带';'
    :param:sql - sql语句
    :param: db  数据库
    return：dataframe数据
    """
    conn = hive.Connection(
        host=ip,
        port=port,
        auth=auth,
        database=db,
        username=user,
        password=password,
    )
    df = pd.read_sql(sql, conn)
    columns = df.columns
    columns_dict = {column: column.split(".")[-1] for column in columns}
    df.rename(columns=columns_dict, inplace=True)
    conn.close()
    return df


# 3.2 Impala
def impala_to_df(sql):
    conn = connect(host=impala_info["host"], port=impala_info["port"],database=impala_info["dbname"])
    cur = conn.cursor()
    cur.execute(sql)
    df = as_pandas(cur)
    cur.close()
    conn.close()
    return df

sql = '''
'''
data = impala_to_df(sql)

# 3.3 Orcle
conn = cx_Oracle.connect('GAEI/Gaei#727@10.8.101.16:1521/PLADB')
cursor= conn.cursor()
sql = ""

#执行SQL语句
cursor.execute(sql)

data = pd.read_sql(sql=sql,con=conn)

# 3.4 Spark
# 3.5 Mysql
HOSTNAME="132.121.82.139"
PORT=3308
USENAME="credit"
PASSWORD="c#rEdi12t"
DATABASE='credit'

conn = pymysql.connect(host=HOSTNAME,user=USENAME,password=PASSWORD,database=DATABASE,port=PORT)

cursor = conn.cursor()

sql = ""

#执行SQL语句
cursor.execute(sql)

data = pd.read_sql(sql=sql,con=conn)

#关闭光标对象
cursor.close()

#关闭数据库连接
conn.close()

# 3.6 CSV
data = pd.read_csv(r"D:\rankingcard.csv",header=0,index_col=0)
# 3.7 Excel
data = pd.read_excel(r"D:\rankingcard.xlsx",header=0,index_col=0)


####################################################################################################
# 4 数据探索
data.head().append(data.tail())
data.shape
data.info()
data.describe()
data.describe().T
data.isnull().sum()

# 数字特征
numeric_features = data.select_dtypes(include=[np.number])
numeric_features.columns
# 类型特征
categorical_features = data.select_dtypes(include=[np.object])
categorical_features.columns

# y分布
y = data['price']
#无界约翰逊分布
plt.figure(1); plt.title('Johnson SU')
sns.distplot(y, kde=False, fit=st.johnsonsu)
#正态分布
plt.figure(2); plt.title('Normal')
sns.distplot(y, kde=False, fit=st.norm)
#对数正态分布
plt.figure(3); plt.title('Log Normal')
sns.distplot(y, kde=False, fit=st.lognorm)

#偏度
sns.distplot(data.skew(),color='blue',axlabel ='Skewness')
#峰度
sns.distplot(data.kurt(),color='orange',axlabel ='Kurtness')

#直方图看频数
plt.hist(data['price'], orientation = 'vertical',histtype = 'bar', color ='red')
plt.show()

#密度曲线
fig = plt.figure(figsize = (10,6))
ax1 = fig.add_subplot(2,1,1)
data['列名'].plot(kind = 'kde',grid = True,style = '-k',title = '密度曲线')
plt.axvline(3*data['列名'].std(),color='r',linestyle="--",alpha=0.8) 
plt.axvline(-3*data['列名'].std(),color='r',linestyle="--",alpha=0.8)

#散点图
plt.scatter(data['列名'].index,data['列名'],color = 'k',marker='.',alpha = 0.3)
plt.xlim([-10,10010])
plt.grid()

#箱型图
fig = plt.figure(figsize = (10,6))
ax1 = fig.add_subplot(2,1,1)
color = dict(boxes='DarkGreen', whiskers='DarkOrange', medians='DarkBlue', caps='Gray')
data['列名'].plot.box(vert=False, grid = True,color = color,ax = ax1,label = '样本数据')


####################################################################################################
# 5 数据预处理
# 5.1 去除重复值
data.drop_duplicates(inplace=True).reset_index(drop=True)

# 5.2 非结构化处理
# 5.2.1 文本格式处理
re.match('pattern', 'string', flags=0)

# 5.2.2 日期格式处理
def lt_process(x):
    if x == '0':
        return 0
    if len(x) == 6:
        return (datetime.datetime.strptime(x,'%Y%m').date() - datetime.datetime.strptime('20200601','%Y%m%d').date()).days
    if len(x) == 8:
        return (datetime.datetime.strptime(x,'%Y%m%d').date() - datetime.datetime.strptime('20200601','%Y%m%d').date()).days

lt = []
df_hff = data[(data['payment_id']==1)&(data['is_active_user']==1)]
for i in lt:
    df_hff[i] = df_hff[i].fillna(0).astype(int).astype(str).apply(lt_process)


# 5.3 缺失值处理
data.isnull().sum()
msno.matrix(data.sample(250))
msno.bar(data.sample(1000))
msno.heatmap(data,figsize=(16, 7))

# 5.3.1 删除特征
for i in data.columns:
    if data[i].isnull().sum()>len(data)*0.5:
        data.drop(i,axis=1,inplace=True)

# 5.3.2 常数填充
data.fillna(0,inplace=True)

for i in data.columns:
    data[i].fillna(data[i].mode()[0])
    data[i].fillna(data[i].mean())


imp_mean = SimpleImputer() #实例化，默认均值填补
imp_median = SimpleImputer(strategy="median") #用中位数填补
imp_0 = SimpleImputer(strategy="constant",fill_value=0) #用0填补
imp_mode = SimpleImputer(strategy = "most_frequent") #用众数填补

for i in data.columns:
    data[i] = imp_mean.fit_transform(i) #fit_transform一步完成调取结果
    data[i] = imp_median.fit_transform(i)
    data[i] = imp_0.fit_transform(i)
    data[i] = imp_mode.fit_transform(i)


# 5.3.3 随机森林填充
def fill_missing_rf(X,y,to_fill):
    '''
    使用随机森林填补一个特征的缺失值的函数

    参数：
    X:要填补的特征矩阵
    y:完整的，没有缺失值的标签
    to_fill:字符串，要填不的那一列的名称
    '''
    #构建我们的新特征矩阵和新标签
    df = X.copy()
    fill = df.loc[:,to_fill]
    df = pd.concat([df.loc[:,df.columns != to_fill],pd.DataFrame(y)],axis=1)

    #找出我们的训练集和测试集
    Ytrain =fill[fill.notnull()]
    Ytest = fill[fill.isnull()]
    Xtrain = df.iloc[Ytrain.index,:]
    Xtest = df.iloc[Ytest.index,:]

    #用随机森林回归来填补缺失值
    from sklearn.ensemble import RandomForestRegressor as rfr
    rfr = rfr(n_estimators=100)
    rfr = rfr.fit(Xtrain,Ytrain)
    Ypredict = rfr.predict(Xtest)

    return Ypredict

X = data.iloc[:]
y = data["SeriousDlqin2yrs"]

y_pred = fill_missing_rf(X,y,"MonthlyIncome")

data.loc[data.loc[:,"MonthlyIncome"].isnull(),"MonthlyIncome"]=y_pred

# 5.3.4 删除样本
data.dropna(subset=['列名'],inplace=True)

# 5.4 异常值处理
# 5.4.1 3σ原则
for i in data.columns:
    data[i] = data[i][np.abs(data[i] - data[i].mean()) <= 3*data[i].std()]

# 5.4.2 箱型图处理
for i in data.columns:
    q1 = data[i].describe['25%']
    q3 = data[i].describe['75%']
    iqr = q3 - q1
    mi = q1 - 1.5*iqr
    ma = q3 + 1.5*iqr
    data[i] = data[i][(data[i] >= mi) & (data[i] <= ma)]





# 5.5 切分训练集
# 5.5.1 随机切分
X = pd.DataFrame(X)
y = pd.DataFrame(y)
X_train, X_vali, Y_train, Y_vali = train_test_split(X,y,test_size=0.3,random_state=420)
model_data = pd.concat([Y_train, X_train], axis=1)
model_data.index = range(model_data.shape[0])
model_data.columns = data.columns
vali_data = pd.concat([Y_vali, X_vali], axis=1)
vali_data.index = range(vali_data.shape[0])
vali_data.columns = data.columns

# 5.5.2 分层采样
split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

for train_index, test_index in split.split(data, data["特征"]):
    strat_train_set = data.loc[train_index]
    strat_test_set = data.loc[test_index]


# 5.6 离散化
# 5.6.1 等距分箱
model_data["cut"], updown = pd.cut(model_data["age"], retbins=True, q=20)


# 5.6.2 等频分箱
model_data["qcut"], updown = pd.qcut(model_data["age"], retbins=True, q=20)


# 5.6.3 卡方分箱
def graphforbestbin(DF, X, Y, n=5,q=20,graph=True):
    """
    自动最优分箱函数，基于卡方检验的分箱
    参数：
    DF: 需要输入的数据
    X: 需要分箱的列名
    Y: 分箱数据对应的标签 Y 列名
    n: 保留分箱个数
    q: 初始分箱的个数
    graph: 是否要画出IV图像
    区间为前开后闭 (]
    """

    DF = DF[[X,Y]].copy()

    DF["qcut"],bins = pd.qcut(DF[X], retbins=True, q=q,duplicates="drop")
    coount_y0 = DF.loc[DF[Y]==0].groupby(by="qcut").count()[Y]
    coount_y1 = DF.loc[DF[Y]==1].groupby(by="qcut").count()[Y]
    num_bins = [*zip(bins,bins[1:],coount_y0,coount_y1)]

    for i in range(q):
        if 0 in num_bins[0][2:]:
            num_bins[0:2] = [(
                num_bins[0][0],
                num_bins[1][1],
                num_bins[0][2]+num_bins[1][2],
                num_bins[0][3]+num_bins[1][3])]
            continue

        for i in range(len(num_bins)):
            if 0 in num_bins[i][2:]:
                num_bins[i-1:i+1] = [(
                    num_bins[i-1][0],
                    num_bins[i][1],
                    num_bins[i-1][2]+num_bins[i][2],
                    num_bins[i-1][3]+num_bins[i][3])]
                break
        else:
            break

    def get_woe(num_bins):
        columns = ["min","max","count_0","count_1"]
        df = pd.DataFrame(num_bins,columns=columns)
        df["total"] = df.count_0 + df.count_1
        df["percentage"] = df.total / df.total.sum()
        df["bad_rate"] = df.count_1 / df.total
        df["good%"] = df.count_0/df.count_0.sum()
        df["bad%"] = df.count_1/df.count_1.sum()
        df["woe"] = np.log(df["good%"] / df["bad%"])
        return df

    def get_iv(df):
        rate = df["good%"] - df["bad%"]
        iv = np.sum(rate * df.woe)
        return iv

    IV = []
    axisx = []
    bins_df = pd.DataFrame()
    while len(num_bins) > n:
        pvs = []
        for i in range(len(num_bins)-1):
            x1 = num_bins[i][2:]
            x2 = num_bins[i+1][2:]
            pv = scipy.stats.chi2_contingency([x1,x2])[1]
            pvs.append(pv)

        i = pvs.index(max(pvs))
        num_bins[i:i+2] = [(
            num_bins[i][0],
            num_bins[i+1][1],
            num_bins[i][2]+num_bins[i+1][2],
            num_bins[i][3]+num_bins[i+1][3])]

        bins_df = pd.DataFrame(get_woe(num_bins))
        axisx.append(len(num_bins))
        IV.append(get_iv(bins_df))

    if graph:
        plt.figure()
        plt.plot(axisx,IV)
        plt.xticks(axisx)
        plt.show()
    return bins_df  

for i in model_data.columns[1:-1]:
    print(i)
    graphforbestbin(model_data,i,"SeriousDlqin2yrs",n=2,q=20)



auto_col_bins = {"RevolvingUtilizationOfUnsecuredLines":6,
                "age":5,
                "DebtRatio":4,
                "MonthlyIncome":3,
                "NumberOfOpenCreditLinesAndLoans":5}

#不能使用自动分箱的变量
hand_bins = {"NumberOfTime30-59DaysPastDueNotWorse":[0,1,2,13]
            ,"NumberOfTimes90DaysLate":[0,1,2,17]
            ,"NumberRealEstateLoansOrLines":[0,1,2,4,54]
            ,"NumberOfTime60-89DaysPastDueNotWorse":[0,1,2,8]
            ,"NumberOfDependents":[0,1,2,3]}

#保证区间覆盖使用 np.inf替换最大值，用-np.inf替换最小值
hand_bins = {k:[-np.inf,*v[:-1],np.inf] for k,v in hand_bins.items()}




bins_of_col = {}

# 生成自动分箱的分箱区间和分箱后的 IV 值
for col in auto_col_bins:
    bins_df = graphforbestbin(model_data,col
                            ,"SeriousDlqin2yrs"
                            ,n=auto_col_bins[col]
                            #使用字典的性质来取出每个特征所对应的箱的数量
                            ,q=20
                            ,graph=False)
    bins_list = sorted(set(bins_df["min"]).union(bins_df["max"]))
    #保证区间覆盖使用 np.inf 替换最大值 -np.inf 替换最小值
    bins_list[0],bins_list[-1] = -np.inf,np.inf
    bins_of_col[col] = bins_list

#合并手动分箱数据
bins_of_col.update(hand_bins)
bins_of_col


# 5.7 编码
'''
对于有序离散特征，尝试 Ordinal (Integer), Binary, OneHot, LeaveOneOut, and Target. Helmert, Sum, BackwardDifference and Polynomial 基本没啥用，但是当你有确切的原因或者对于业务的理解的话，可以进行尝试。
对于回归问题而言，Target 与 LeaveOneOut 方法可能不会有比较好的效果。
LeaveOneOut、 WeightOfEvidence、 James-Stein、M-estimator 适合用来处理高基数特征。Helmert、 Sum、 Backward Difference、 Polynomial 在机器学习问题里的效果往往不是很好(过拟合的原因)
'''

# 5.7.1 woe编码
# use target encoding to encode two categorical features
enc = WOEEncoder(cols='需要编码字段')

# transform the datasets
encoded_train = enc.fit_transform(X_train,Y_train)
encoded_test = enc.transform(X_vali)



def get_woe(df,col,y,bins):
    df = df[[col,y]].copy()
    df["cut"] = pd.cut(df[col],bins)
    bins_df = df.groupby("cut")[y].value_counts().unstack()
    woe = bins_df["woe"] = np.log((bins_df[0]/bins_df[0].sum())/(bins_df[1]/bins_df[1].sum()))
    return woe

#将所有特征的WOE存储到字典当中
woeall = {}
for col in bins_of_col:
    woeall[col] = get_woe(model_data,col,"SeriousDlqin2yrs",bins_of_col[col])

#不希望覆盖掉原本的数据，创建一个新的DataFrame，索引和原始数据model_data一模一样
model_woe = pd.DataFrame(index=model_data.index)
#将原数据分箱后，按箱的结果把WOE结构用map函数映射到数据中
model_woe["age"] = pd.cut(model_data["age"],bins_of_col["age"]).map(woeall["age"])
#对所有特征操作可以写成：
for col in bins_of_col:
    model_woe[col] = pd.cut(model_data[col],bins_of_col[col]).map(woeall[col])
#将标签补充到数据中
model_woe["SeriousDlqin2yrs"] = model_data["SeriousDlqin2yrs"]
#这就是我们的建模数据了
model_woe.head()


# 5.7.2 序数编码
encoder = OrdinalEncoder(cols = ['需要编码的字段', '需要编码的字段'], 
                         handle_unknown = 'value', 
                         handle_missing = 'value').fit(X_train,Y_train) # 在训练集上训练
# 将 handle_unknown设为‘value’，即测试集中的未知特征值将被标记为-1
# 将 handle_missing设为‘value’，即测试集中的缺失值将被标记为-2
# 其他的选择为：‘error’：即报错；‘return_nan’：即未知值/缺失之被标记为nan 
encoded_train = encoder.transform(X_train) # 转换训练集
encoded_test = encoder.transform(X_vali) # 转换测试集

# 5.7.3 独热编码
encoder = OneHotEncoder(cols=['需要编码的字段', '需要编码的字段'], 
                        handle_unknown='indicator', 
                        handle_missing='indicator', 
                        use_cat_names=True).fit(X_train,Y_train) # 在训练集上训练
encoded_train = encoder.transform(X_train) # 转换训练集
encoded_test = encoder.transform(X_vali) # 转换测试集


# 5.7.4 目标编码
encoder = TargetEncoder(cols=['需要编码的字段','需要编码的字段'], 
                        handle_unknown='value',  
                        handle_missing='value').fit(X_train,Y_train) # 在训练集上训练
encoded_train = encoder.transform(X_train) # 转换训练集
encoded_test = encoder.transform(X_vali) # 转换测试集


# 5.7.5 Binary编码
# 使用binary编码的方式来编码类别变量
encoder = BinaryEncoder(cols=['需要编码的字段']).fit(data)

# 转换数据
numeric_dataset = encoder.transform(data)


# 5.7.6 Catboost编码
enc = CatBoostEncoder()
obtained = enc.fit_transform(X_train, Y_train)

# For testing set, use statistics calculated on all the training data.
# See: CatBoost: unbiased boosting with categorical features, page 4.
obtained = enc.transform(X_vali)


# 5.8 采样
# 5.8.1 上采样
scy = SMOTENC(sampling_strategy={1:20000},random_state=42,categorical_features=[0,9])
X_train, Y_train = scy.fit_sample(X_train,Y_train)

# 5.8.2 下采样
xcy = RandomUnderSampler(sampling_strategy={0:200000},random_state=42)
X_train, Y_train = xcy.fit_sample(X_train,Y_train)


# 5.9 无量纲化
# 5.9.1 归一化
scaler = MinMaxScaler() #实例化
scaler = scaler.fit(data) #fit，在这里本质是生成min(x)和max(x)
data = scaler.transform(data) #通过接口导出结果

# 5.9.2 标准化
scaler = StandardScaler() #实例化
scaler.fit(data) #fit，本质是生成均值和方差
data = scaler.transform(data) #通过接口导出结果


####################################################################################################
# 6 特征工程
# 6.1 特征筛选
# 6.1.1 过滤法
# 6.1.1.1 方差过滤
selector = VarianceThreshold() #实例化，不填参数默认方差为0
data = selector.fit_transform(data) #获取删除不合格特征之后的新特征矩阵
data = VarianceThreshold(np.median(data.var().values)).fit_transform(data)
#若特征是伯努利随机变量，假设p=0.8，即二分类特征中某种分类占到80%以上的时候删除特征
X_train = VarianceThreshold(.8 * (1 - .8)).fit_transform(X_train = SelectKBest(chi2, k=300).fit_transform(X_train, Y_train)
)

# 6.1.1.2 卡方过滤
#假设在这里我一直我需要300个特征
chivalue, pvalues_chi = chi2(X_train,Y_train)
#k取多少？我们想要消除所有p值大于设定值，比如0.05或0.01的特征：
k = chivalue.shape[0] - (pvalues_chi > 0.05).sum()

X_train = SelectKBest(chi2, k=k).fit_transform(X_train, Y_train)

# 6.1.1.3 F检验
X_fsvar = pd.DataFrame()
F, pvalues_f = f_classif(X_fsvar,y)

k = F.shape[0] - (pvalues_f > 0.05).sum()
X_fsF = SelectKBest(f_classif, k='填写具体的k').fit_transform(X_fsvar, y)
cross_val_score(RFC(n_estimators=10,random_state=0),X_fsF,y,cv=5).mean()

# 6.1.1.4 互信息法
from sklearn.feature_selection import mutual_info_classif as MIC
result = MIC(X_fsvar,y)
k = result.shape[0] - sum(result <= 0)
#X_fsmic = SelectKBest(MIC, k=填写具体的k).fit_transform(X_fsvar, y)
#cross_val_score(RFC(n_estimators=10,random_state=0),X_fsmic,y,cv=5).mean() Tsai Tsai


# 6.1.1.5 相关性过滤
corr_matrix = data.corr()

# 6.1.1.5 IV值过滤

# 6.1.2 嵌入法
# 6.1.3 包装法

# 6.2 特征创造


####################################################################################################
# 7 训练模型
# 7.1 时间序列
# 7.1.1 hw
def fcast_exp3(train, step):
    """

    Holter Winters模型

    :param train: 训练数据集
    :param step: 训练步长
    :return: 预测结果
    """
    if sum(train) == 0:
        return [0]*step
    if len(train)>24 and min(train) > 0:
        ets3 = ExponentialSmoothing(train, trend="add", seasonal="mul", seasonal_periods=12)
    elif len(train)>24:
        ets3 = ExponentialSmoothing(train, trend="add", seasonal="add", seasonal_periods=12)
    elif len(train)>12:
        ets3 = ExponentialSmoothing(train, trend="add", seasonal_periods=12)
    elif len(train)>1:
        ets3 = ExponentialSmoothing(train, trend="add")
    else:
        train = [0] + train
        ets3 = ExponentialSmoothing(train, trend="add")

    r3 = ets3.fit()
    pred = r3.forecast(step)
    fitted_fcast = r3.fittedvalues
    trend = r3.trend
    season = r3.season
    level = r3.level
    return pred


def fcast_exp3(df_train,n):
    ets3 = ExponentialSmoothing(df_train, trend='add', seasonal='add', seasonal_periods=12)
    r3 = ets3.fit()
    pred3 = r3.predict(start=len(df_train), end=len(df_train)-1+n)
    return pred3


# 7.1.2 arima
def arima(y,r,n):
    p = d = q = range(0, r)
    pdq = list(itertools.product(p, d, q))
    res = pd.DataFrame(columns=('param', 'aic'))
    for param in pdq:
        try:
            mod = ARIMA(y,order=param)
            results = mod.fit()
            print('ARIMA{} - AIC:{}'.format(param, results.aic))
            res = res.append([{'param':param,'aic':results.aic}], ignore_index=True)
        except:
            res = res.append([{'param':param,'aic':None}], ignore_index=True)
    try:
        re = res['param'][res['aic'] == res['aic'].min()].values[0]
        mod = ARIMA(y,order=re)
        results = mod.fit()
        f = results.forecast(n)[0]
    except:
        f = [0.0]*n
    return f


# 7.1.3 auto_arima
def fcast_arima(df_train,p,d,q,m,t):
    arima = auto_arima(df_train, start_p=0, d=0, start_q=0, max_p=p, max_d=d, max_q=q, start_P=0, D=0, start_Q=0, max_P=p, max_D=d, max_Q=q, m=m,
                       error_action='ignore')
    arima.fit(df_train)
    return arima.predict(n_periods=t)

# 7.1.4 SARIMAX
def sarima(y,r,n):
    p = d = q = range(0, r)
    pdq = list(itertools.product(p, d, q))
    seasonal_pdq = [(x[0], x[1], x[2], 12) for x in pdq]
    res = pd.DataFrame(columns=('param', 'param_seasonal', 'aic'))
    for param in pdq:
        for param_seasonal in seasonal_pdq:
            try:
                mod = sm.tsa.statespace.SARIMAX(y,
                                                order=param,
                                                seasonal_order=param_seasonal,
                                                enforce_stationarity=False,
                                                enforce_invertibility=False)
 
                results = mod.fit()
 
                print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
                res = res.append([{'param':param,'param_seasonal':param_seasonal,'aic':results.aic}], ignore_index=True)
            except:
                res = res.append([{'param':param,'param_seasonal':param_seasonal,'aic':None}], ignore_index=True)
    try:
        re = res['param'][res['aic'] == res['aic'].min()].values[0]
        sre =  res['param_seasonal'][res['aic'] == res['aic'].min()].values[0]
        mod = sm.tsa.statespace.SARIMAX(y,
                                    order=re,
                                    seasonal_order=sre,
                                    enforce_stationarity=False,
                                    enforce_invertibility=False)
 
        results = mod.fit()
        f = results.forecast(n)
    except:
        f = [0.0]*n
    return f


def fcast_sarimax(ts,exog_vars,test_exog_vars,n):
    # Grid Search
    p = d = q = range(0,2) # p, d, and q can be either 0, 1, or 2
    pdq = list(itertools.product(p,d,q)) # gets all possible combinations of p, d, and q
    p2 = d2 = q2 = range(0, 2) # second set of p's, d's, and q's
    pdq2 = list(itertools.product(p2,d2,q2)) # simular too code above but for seasonal parameters
    s = 12 # here I use twelve but the number here is representative of the periodicty of the seasonal cycle
    pdqs2 = [(c[0], c[1], c[2], s) for c in pdq2]
    combs = {}
    aics = []
    # Grid Search Continued
    for combination in pdq:
        for seasonal_combination in pdqs2:
            try:
                model = sm.tsa.statespace.SARIMAX(ts, order=combination, seasonal_order=seasonal_combination,
                                                  exog=exog_vars,
                                                  enforce_stationarity=False,
                                                  enforce_invertibility=False)
                model = model.fit()
                combs.update({model.aic : [combination, seasonal_combination]})
                aics.append(model.aic)

            except:
                continue

    best_aic = min(aics)
    # Modeling and forcasting
    model = sm.tsa.statespace.SARIMAX(ts, order=combs[best_aic][0], seasonal_order=combs[best_aic][1],
                                      exog=exog_vars,
                                      enforce_stationarity=False,
                                      enforce_invertibility=False)
    model = model.fit()
    return model.forecast(n,exog=test_exog_vars)


####################################################################################################

# 8 模型评价
# 8.1 训练集评价
# 8.1.1 交叉验证
cross_val_score(estimator=model, X=X, y=y, cv=3,scoring='roc_auc')

# 8.2 测试集评价
# 8.2.1 分类模型评价
# 8.2.1.1 精确率
print('ACC:',accuracy_score(y_true,y_pred))
# 8.2.1.2 准确率
print('Precision:',precision_score(y_true,y_pred))
# 8.2.1.3 召回率
print('Recall:',recall_score(y_true,y_pred))
# 8.2.1.4 f1
print('F1-score:',f1_score(y_true,y_pred))
# 8.2.1.5 AUC
print('AUC score:',roc_auc_score(y_true,y_scores))
# 8.2.1.6 混淆矩阵
confusion_matrix(y_true,y_pred)

# 8.2.2 回归模型评价
# 8.2.2.1 MSE 均方误差
print('MES:',mean_squared_error(y_pred,y_true))
# 8.2.2.2 RMSE 均方根误差
print('RMSE',np.sqrt(mean_squared_error(y_pred,y_true)))
# 8.2.2.3 MAE 平均绝对误差
print('MAE:',mean_absolute_error(y_pred,y_true))
# 8.2.2.4 MAPE 平均绝对百分比误差
def mape(y_true,y_pred):
    return np.mean(np.abs((y_pred-y_true)/y_true))

print('MAPE:',mape(y_true,y_pred))



# 9 模型优化
# 9.1 学习曲线
def plot_learning_curve(estimator,title, X, y,
ax=None, #选择子图
ylim=None, #设置纵坐标的取值范围
cv=None, #交叉验证
n_jobs=None #设定索要使用的线程
):
    train_sizes, train_scores, test_scores = learning_curve(estimator, X, y
    ,shuffle=True
    ,cv=cv
    # ,random_state=420
    ,n_jobs=n_jobs)
    if ax == None:
        ax = plt.gca()
    else:
        ax = plt.figure()
    ax.set_title(title)
    if ylim is not None:
        ax.set_ylim(*ylim)
    ax.set_xlabel("Training examples")
    ax.set_ylabel("Score")
    ax.grid() #绘制网格，不是必须
    ax.plot(train_sizes, np.mean(train_scores, axis=1), 'o-'
        , color="r",label="Training score")
    ax.plot(train_sizes, np.mean(test_scores, axis=1), 'o-'
        , color="g",label="Test score")
    ax.legend(loc="best")
    return ax

Xtrain = 1
Ytrain = 1
cv = KFold(n_splits=5, shuffle = True, random_state=42)
plot_learning_curve(XGBR(n_estimators=100,random_state=420),"XGB",Xtrain,Ytrain,ax=None,cv=cv)
plt.show()


####################################################################################################
# 10 结果转化
####################################################################################################
# 11 保存结果
####################################################################################################
# 12 模型应用
def main(dmon):
    print('start')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('参数输入错误！')
        sys.exit()
    else:
        dmon = int(sys.argv[1])
        print('当前执行数据月份：', dmon)
        logging.info('当前执行数据月份：' + str(dmon))

        try:
            main(dmon)
        except:
            s = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + \
                traceback.format_exc()
            f = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + \
                '脚本执行出错！'
            print(s)
            print(f)
            logging.error(s)
