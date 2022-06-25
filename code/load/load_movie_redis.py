# 作用：向redis中放入movieId2movieName
# 启动参数：三个
#   - redis_host
#   - redis_port
#   - file_path
from pandas.core import generic
import numpy as np
import pandas as pd
import sys
import redis
import random
import json

def redis_connect(host="121.36.88.159", port=6379):    
    pool = redis.ConnectionPool(host=host,port=port,decode_responses=True,password='Kd7Jdddd16@6djie8gce342NWM9znN4$V')
    print(f'Connect to {host} succeed.')
    return pool

def getArgs():
    argv = sys.argv[1:]
    return argv[0], argv[1], argv[2]

def load_file(filename):
    print('Load %s ...' % filename)
    dataSet = pd.read_csv(filename)
    for line in dataSet.itertuples():
        yield list(line[1::])
    print('Load %s success!' % filename)

if __name__=='__main__':
    host, port, file_path = getArgs()
    redis_pool = redis_connect(host, port)
    nextf = load_file(file_path)
    list_name = []
    for i, nowl in enumerate(load_file(file_path)):
        # nowl = line.split(",")
        # 向hbase中写入

        # 向redis中写入
        print(nowl[0], nowl[1], nowl[3])
        r = redis.Redis(connection_pool=redis_pool)
        r.delete(f"movieId2movieYear_{nowl[0]}")
        r.set(f"movieId2movieYear_{nowl[0]}", nowl[3])
        r.delete(f"movieId2movieTitle_{nowl[0]}")
        r.set(f"movieId2movieTitle_{nowl[0]}",str(nowl[1]))
        genres = []
        r.delete(f"movie2genres_movieId_{nowl[0]}")
        for i in range(4,len(nowl)):
            # print(nowl[i],end=" || ")
            if int(nowl[i])==1:
                r.rpush(f"movie2genres_movieId_{nowl[0]}",i-4)
        print()
        r.close()




