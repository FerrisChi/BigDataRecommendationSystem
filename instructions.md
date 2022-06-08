## 运行流程

​	<u>**表示在所有结点都需执行，init表示在初始化系统的时候执行，其余指令只需在主节点上执行。*</u>

1. 启动HDFS

   `start-all.sh`

2. 启动zookeeper（*）

   `zkServer.sh start`

3. 启动HBase

   `start-hbase.sh`

4. 配置HBase Thrift连接，以便python中的happybase库能够连接Hbase

   `hbase-daemon.sh start thrift`

5. 在HBase中创建对应的表（init）

   `create 'movie_records','details'`

   * 查看HBase数据：`scan 'movie_records',{LIMIT=>5}` 查看movie_records表的前5行

6. 启动load_train_ratings_hbase.py（init）

   `python .\load_train_ratings_hbase.py 121.36.88.159 9090 "movie_records" "../../data/json_train_ratings.json"`

7.  启动redis

   `redis-server /root/redis-6.0.6/redis.conf`

   * 查看redis数据：

     * 进入redis
       `redis-cli -h 127.0.0.1 -p 6379`

     * 查看redis中的key
       `keys [pattern]`
       e.g.: `keys *` （查看所有的keys）
      
     * 获取key对应的value
       `get [key]`
       e.g.: `get 'movieId2movieTitle_1'`

     * 查看redis中的表(list)
       `lrange [表的名字] 0 -1`

8. 启动load_movie_redis.py（init）

   可在本地执行

   `python .\load_movie_redis.py 121.36.88.159  6379 "../../data/movies.csv"`

9. 启动Kafka（*）

   `kafka-server-start.sh /home/modules/kafka_2.11-0.10.2.2/config/server.properties`

10. 创建 Kafka Topic

    `kafka-topics.sh --zookeeper node001:2181 --create --topic movie_rating_records --partitions 1 --replication-factor 1`

11. 启动generatorRecord.py（这个程序会一直运行，不需要等待停止）

    最好在服务器上运行，若要本地Windows/macOS运行，需额外配置kafka外网连接

    `python3 /root/code/load/generatorRecord.py -h node001:9092  -f "/root/data/json_test_ratings.json"`

12. 启动hbase2spark、kafkaStreaming、recommend

    * `spark-submit --class hbase2spark --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/spark-sparkstreaming-recommend.jar`
    * `spark-submit --class kafkaStreaming --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/spark-sparkstreaming-recommend.jar`
    * `spark-submit --class recommend --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/spark-sparkstreaming-recommend.jar`

13. 启动recommend_server.py

    可在本地执行

    `python code/server-client/recommend_server.py "121.36.88.159" 6379 23456`

14. 启动recommend_client.py

    可在本地执行

    `python code/server-client/recommend_client.py 127.0.0.1 23456`



## Instructions

【写在前面】
    下面出现的121.36.88.159统一修改为自己主节点的公网ip地址
    node001统一修改为自己主节点名称
    spark-sparkstreaming-recommend.jar统一修改为自己的jar包名称

```
#启动HDFS
/home/modules/hadoop-2.7.7/sbin/start-all.sh

#关闭HDFS
/home/modules/hadoop-2.7.7/sbin/stop-all.sh

#启动zookeeper（所有节点）
/usr/local/zookeeper/bin/zkServer.sh start

#关闭zookeeper（所有节点）
/usr/local/zookeeper/bin/zkServer.sh stop

#启动HBase（所有结点）
/usr/local/hbase/bin/start-hbase.sh

#关闭HBase
/usr/local/hbase/bin/stop-hbase.sh

#配置HBase Thrift连接
/usr/local/hbase/bin/hbase-daemon.sh start thrift

#在HBase shell中创建对应的表
create 'movie_records','details'

#在HBase shell中查看刚写入的数据
scan 'movie_records'

#启动kafka（所有节点）
/home/modules/kafka_2.11-0.10.2.2/bin/kafka-server-start.sh /home/modules/kafka_2.11-0.10.2.2/config/server.properties

#创建kafka topic
/home/modules/kafka_2.11-0.10.2.2/bin/kafka-topics.sh --zookeeper node001:2181 --create --topic movie_rating_records --partitions 1 --replication-factor 1

#删除kafka topic
/home/modules/kafka_2.11-0.10.2.2/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic movie_rating_records

#查看kafka topic
/home/modules/kafka_2.11-0.10.2.2/bin/kafka-topics.sh --list --zookeeper localhost:2181

#查看kafka topic消息数量
/home/modules/kafka_2.11-0.10.2.2/bin/kafka-run-class.sh  kafka.tools.GetOffsetShell --broker-list node001:9092 --topic movie_rating_records --time -1

#启动redis
redis-server redis-6.0.6/redis.conf

#进入redis
redis-cli -h 127.0.0.1 -p 6379

#查看redis中的key
keys 【pattern】

#查看redis中的表
lrange 【表的名字】 0 -1

#启动generatorRecord.py（最好在服务器上运行，若要本地Windows/macOS运行，需额外配置kafka外网连接）
python3 generatorRecord.py -h node001:9092  -f "../../data/json_test_ratings.json"

#启动load_movie_redis.py（可以在本地Windows/macOS运行）
python .\load_movie_redis.py 121.36.88.159  6379 "../../data/movies.csv"

#启动load_train_ratings_hbase.py（可以在本地Windows/macOS运行）
python .\load_train_ratings_hbase.py 121.36.88.159 9090 "movie_records" "../../data/json_train_ratings.json"

#spark提交hbase2spark任务
/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class hbase2spark --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 spark-sparkstreaming-recommend.jar

#spark提交kafkaStreaming任务
/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class kafkaStreaming --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 spark-sparkstreaming-recommend.jar

#spark提交recommend任务
/root/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class recommend --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 spark-sparkstreaming-recommend.jar

#启动推荐系统server（可以在本地Windows/macOS运行）
python .\recommend_server.py "121.36.88.159" 6379 23456

#启动推荐系统client（和recommend_server.py端同机器运行）
python .\recommend_client.py 127.0.0.1 23456
```



