import pandas as pd
from pyhive import hive
from datetime import datetime
from util.common import read_config


#读取配置
hiveinfo = read_config('effective_hiveinfo')

def hive_execute(sql, db = 'ods_g063_grt_all_db'):
    """

    在hive里面执行sql语句

    :param:sql - sql语句
    :param: db  数据库名称

    """
    conn = hive.Connection(
        host=hiveinfo["ip"],
        port=hiveinfo["port"],
        auth=hiveinfo["auth"],
        database=db,
        username=hiveinfo["user"],
        password=hiveinfo["password"]
    )
    cur = conn.cursor()
    hive_sqls = sql.split(";")
    for exec_sql in hive_sqls:
        print(exec_sql)
        Start_time = datetime.now()
        print("Start_time:", Start_time)
        cur.execute(exec_sql)
        End_time = datetime.now()
        print("End_time:", datetime.now())
        print("本次执行时长为：" + str((End_time - Start_time).seconds) + "s\n")
    cur.close()
    conn.close()


def hive_to_df(sql, db = 'ods_g063_grt_all_db'):
    """
    功能：将hive数据转换dataframe
    注意：sql不需要带';'
    :param:sql - sql语句
    :param: db  数据库
    return：dataframe数据
    """
    conn = hive.Connection(
        host=hiveinfo["ip"],
        port=hiveinfo["port"],
        auth=hiveinfo["auth"],
        database=db,
        username=hiveinfo["user"],
        password=hiveinfo["password"]
    )
    print(sql)
    df = pd.read_sql(sql, conn)
    columns = df.columns
    columns_dict = {column: column.split(".")[-1] for column in columns}
    df.rename(columns=columns_dict, inplace=True)
    conn.close()
    return df