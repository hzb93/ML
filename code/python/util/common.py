import os
import configparser

def read_config(option):
    cfgpath=os.path.join(os.getcwd(),"config/force.ini")
    conf = configparser.ConfigParser()
    conf.read(cfgpath)
    return dict(conf.items(option))


def location(x):
    for i,a in enumerate(x):
        if a>0:
            return i
    return len(x)


def result_handel(result):
    if type(result) == list:
        result_list = []
        for i in result:
            if i > 0:
                result_list.append(int(round(i,0)))
            else:
                result_list.append(0)
        return result_list
    else:
        if result > 0:
            return int(round(result,0))
        else:
            return 0