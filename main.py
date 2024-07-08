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



import matplotlib
matplotlib.use("module://imgcat")
import matplotlib.pyplot as plt
# 数据
x = [1, 2, 3, 4, 5]  # x轴数据
y = [2, 4, 6, 8, 10]  # y轴数据
# 绘制折线图
plt.plot(x, y)
# 设置图表标题和坐标轴标签
plt.title("Simple Line Chart")
plt.xlabel("X-axis")
plt.ylabel("Y-axis")
# 显示图表
plt.show()


from imgcat import imgcat
imgcat(open("/model/devmodel/hzb/intent/线索口径.jpg"))