# BigDataRecommendationSystem

This Recommendation system is a Big data course work meant to use users' behavior to recommend movies for them. The project is based on Lambda architecture, with Kafka, Hbase, Spark, Spark Streaming, Redis as its components.

![image-20220702180535653](images/README/arch.png)



## Requirement

In the project we use 4 ECSs as the cluster and run Hadoop on them. Requirements are as follows.

* Cluster requirements:
  * OpenJDK8U-jdk_aarch64_linux_openj9_8u292b10_openj9-0.26.0
  * hadoop-2.7.7
  * kafka_2.11-0.10.2.2
  * spark-2.1.1 (scala-2.11.8)
  * redis-6.0.6
  * zookeeper-
  * hbase-2.0.2
* Code project requirements are contained in the pom.xml and compiled in Intellj with SDK version  `corretto-1.8` and Maven version `3.8.1`



## structure

* In `./code`，we provide 3 versions of recommendation system. 
  * In `./code/load`, user records are generated (Flume not implemented) and sent to Kafka and  history records are sent and saved in Hbase, while movie info is saved in Redis.
  * In `./code/server-client`, client and server are proviede. Note that the `_md` version is for system in `select` version (since additional feature is used for rating).
  * In `./code/spark-sparkstreaming-recommend(select)`, system with 1 additional feature is used.
  * In `./code/spark-sparkstreaming-recommend(kappa)`, system is developed in Kappa architecture.(Not tested on cluster LOL)
  * In `./code/spark-sparkstreaming-recommend(delta)`, we tried the SOTA arch Delta but bugs are encountered.
* In `./data`, we provide movie information and user records.



## Instructions

### 运行流程

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

   输入`hbase shell`进入hbase命令行界面

   `create 'movie_records','details'`

   * 查看HBase数据：`scan 'movie_records',{LIMIT=>5}` 查看movie_records表的前5行

6. 启动load_train_ratings_hbase.py（init）

   `python .\load_train_ratings_hbase.py 121.36.88.159 9090 "movie_records" "../../data/json_train_ratings.json"`

7. 启动redis

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

   `python .\load_movie_redis.py 121.36.12.46  6379 "../../data/movies.csv"`

9. 启动Kafka（*）

   `kafka-server-start.sh /home/modules/kafka/config/server.properties`

   * 查看Kafka运行状态：Kafka默认端口为**9092**，可以使用命令：netstat -anlpt | grep 9092 或者 lsof -i:9092 来查看9092端口占用情况

10. 创建 Kafka Topic（init）

    `kafka-topics.sh --zookeeper ljj-2019213687-0001:2181 --create --topic movie_rating_records --partitions 1 --replication-factor 1`

    * 查看Kafka Topic中的消息数量：

      `kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list ljj-2019213687-0001:9092 --topic movie_rating_records --time -1`

    * 删除kafka topic：
      `kafka-topics.sh --zookeeper localhost:2181 --delete --topic movie_rating_records`

      清空kafka或某一topic数据：https://www.cnblogs.com/swordfall/p/10014300.html

    * 查看kafka topic：
      `kafka-topics.sh --list --zookeeper localhost:2181`

11. 启动generatorRecord.py（这个程序会一直运行，不需要等待停止）

    最好在服务器上运行，若要本地Windows/macOS运行，需额外配置kafka外网连接

    `python3 /root/code/load/generatorRecord.py -h ljj-2019213687-0001:9092  -f "/root/data/json_test_ratings.json"`

12. 启动hbase2spark、kafkaStreaming、recommend

    * `spark-submit --class hbase2spark --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/cjj/lastExam.jar`

    * `spark-submit --class kafkaStreaming --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/cjj/lastExam.jar`

    * `spark-submit --class recommend --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/cjj/lastExam.jar`

    * delta:

      ```
      spark-submit --class kStream2delta --master yarn --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/spark-sparkstreaming-recommend_d.jar
      
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1
      --packages io.delta:delta-core_2.12:1.2.1
      ```

13. 启动recommend_server.py

    可在本地执行

    `python code/server-client/recommend_server.py "121.36.88.159" 6379 23456`

    `python code/server-client/recommend_server.py "121.36.12.46" 6379 23456`

14. 启动recommend_client.py

    可在本地执行

    `python code/server-client/recommend_client.py 127.0.0.1 23456`

* 密码：Kd7Jdddd16@6djie8gce342NWM9znN4$V

### Reference Instructions

【写在前面】
    下面出现的121.36.88.159统一修改为自己主节点的公网ip地址
    node001统一修改为自己主节点名称
    spark-sparkstreaming-recommend.jar统一修改为自己的jar包名称

```bash
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
