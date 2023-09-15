# 导库
import os
import sys
import yaml
import warnings
from util.logutil import LogUtil

# 设置
warnings.filterwarnings("ignore")
LogUtil.init_log()
logger = LogUtil.logger

# 读取配置文件内容
yaml_path = os.path.join(os.getcwd(),'config/config.yaml')
with open(yaml_path,"r",encoding="utf-8") as f:
    cfg = yaml.load(f,Loader=yaml.FullLoader)

def main():
    # 1 获取数据
    pass


if __name__ == '__main__':
    main()
