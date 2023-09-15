import os
import pandas as pd
from pyhive import hive
from datetime import datetime
import yaml
from sqlalchemy import create_engine
import pymysql
import paramiko
from urllib import parse
import signal
from retrying import retry
pymysql.install_as_MySQLdb()


# 定义一个自定义的异常
class TimeoutException(Exception):
    pass
# 信号处理函数，接收到SIGALRM信号时抛出TimeoutException异常
def handler(signum, frame):
    raise TimeoutException("Timed out!")
# 设置定时器，10秒后向进程发送SIGALRM信号
signal.signal(signal.SIGALRM, handler)


#读取配置
yaml_path = os.path.join(os.getcwd(),'config/config.yaml')
with open(yaml_path,"r",encoding="utf-8") as f:
    cfg = yaml.load(f,Loader=yaml.FullLoader)

@retry(wait_fixed=2000, stop_max_attempt_number=3)
def hive_execute(sql,t=1200):
    """
    在hive里面执行sql语句

    Args:
        sql (str): sql语句
    """

    sqls = sql.split(";;")
    for exec_sql in sqls:
        print(exec_sql)
        Start_time = datetime.now()
        print("Start_time:", Start_time)
        conn = hive.Connection(
                host=cfg['hive']['host'],
                port=cfg['hive']['port'],
                auth=cfg['hive']['auth'],
                database=cfg['hive']['database'],
                username=cfg['hive']['username'],
                password=cfg['hive']['password']
            )
        cur = conn.cursor()
        signal.alarm(t)
        cur.execute(exec_sql)
        # 关闭定时器
        signal.alarm(0)        
        cur.close()
        conn.close()
        End_time = datetime.now()
        print("End_time:", datetime.now())
        print("本次执行时长为：" + str((End_time - Start_time).seconds) + "s\n")
    

@retry(wait_fixed=2000, stop_max_attempt_number=3)
def hive_to_df(sql,t=600):
    """
    将hive数据转换dataframe

    Args:
        sql (str): sql语句

    Returns:
        dataframe: sql语句执行结果
    """
    print(sql)
    conn = hive.Connection(
            host=cfg['hive']['host'],
            port=cfg['hive']['port'],
            auth=cfg['hive']['auth'],
            database=cfg['hive']['database'],
            username=cfg['hive']['username'],
            password=cfg['hive']['password']
        )
    signal.alarm(t)
    df = pd.read_sql(sql, conn)
    # 关闭定时器
    signal.alarm(0)
    conn.close()
    columns = df.columns
    columns_dict = {column: column.split(".")[-1] for column in columns}
    df.rename(columns=columns_dict, inplace=True)
    return df


@retry(wait_fixed=2000, stop_max_attempt_number=3)
def df_to_hive(df, table_name, mode, t=600):
    """
    将dataframe写入hive

    Args:
        df (dataframe): 要写入hive的数据
        table_name (str): hive表名
        mode (str): 表存在时处理方式
    """
    host=cfg['hive']['host']
    port=cfg['hive']['port']
    auth=cfg['hive']['auth']
    database=cfg['hive']['database']
    username=cfg['hive']['username']
    password=cfg['hive']['password']
    password = parse.quote_plus(password)

    engine = create_engine(f"hive://{username}:{password}@{host}:{port}/{database}", connect_args={"auth": "LDAP"})
    Start_time = datetime.now()
    print("Start_time:", Start_time)
    signal.alarm(t)
    df.to_sql(table_name, con=engine, if_exists=mode, index=False, method="multi")
    # 关闭定时器
    signal.alarm(0)
    End_time = datetime.now()
    print("End_time:", datetime.now())
    print("本次执行时长为：" + str((End_time - Start_time).seconds) + "s\n")


@retry(wait_fixed=2000, stop_max_attempt_number=3)
def mysql_execute(sql):
    """
    在mysql里面执行sql语句

    Args:
        sql (str): 要执行的sql语句
    """

    conn = pymysql.Connection(
        host =cfg['mysql']['host'],
        port=cfg['mysql']['port'],
        database=cfg['mysql']['database'],
        user=cfg['mysql']['username'],
        password=cfg['mysql']['password']
    )
    cur = conn.cursor()
    sqls = sql.split(";")
    for exec_sql in sqls:
        print(exec_sql)
        Start_time = datetime.now()
        print("Start_time:", Start_time)
        cur.execute(exec_sql)
        End_time = datetime.now()
        print("End_time:", datetime.now())
        print("本次执行时长为：" + str((End_time - Start_time).seconds) + "s\n")
    cur.close()
    conn.close()

@retry(wait_fixed=2000, stop_max_attempt_number=3)
def df_to_mysql(data, table_name):
    """
    将dataframe写入mysql

    Args:
        data (dataframe): 要写入mysql的数据
        table_name (str): mysql表名
    """
    host =cfg['mysql']['host']
    port=cfg['mysql']['port']
    database=cfg['mysql']['database']
    user=cfg['mysql']['username']
    password=cfg['mysql']['password']
    
    engine = create_engine(f"mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset=utf8")
    data.to_sql(table_name, con=engine, if_exists='append', index=False)


def push_to_grt():
    """
    将hive数据推送到前置仓
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy)
    client.connect(hostname="10.88.20.147", port=22, username="grtdata", password="grtdata@2021#")
    command = """
            . ~/.bash_profile;
            /usr/local/python3/bin/python3.7 /home/grtdata/run_datax.py --table=rec_rev_model_output --preSql="truncate table rec_rev_model_output"
        """
    command2 = "ll;"
    stdin, stdout, stderr = client.exec_command(command)
    # 读取标准输出
    while True:
        line = stdout.readline(1024)
        if line == '':
            break
        else:
            print(line, end="")
    # 错误输出
    print(stderr.read().decode("utf-8"))