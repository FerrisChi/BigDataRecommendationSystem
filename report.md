

## 基础实验：

* 安装Kafka：

  ![image-20220530170135553](images/basic/1.png)

* 确认Redis运行成功：

  ![image-20220530170403867](images/basic/2.png)

* 将movie.csv导入Redis中：

  ![image-20220625202623540](images/basic/7.png)
  
* 执行结果：

  ![image-20220530212046012](images/basic/3.png)

## 选做：

* (升级spark版本至3.2.1：可以看到Scala版本为2.12.15)

  ![image-20220610235431749](images/1.png)

* 验证spark：

  ![image-20220611000059034](images/final/2.png)
  
* client截图：

  ![image-20220626022503970](images/basic/8.png)

* server截图：（输出了CF model的推荐信息）

  ![image-20220626022548304](images/basic/9.png)

## 提高：

* server截图：

  ![image-20220626142707989](images/final/4png)

* client截图：

  ![image-20220626142418978](images/final/3.png)

* 版本：
  * hive-2.1.1
  * hadoop-2..7.7
  * hbase-2.0.2 => hbase-2.4.11
  * kafka-2.11-0.10.22 => 2.12-3.2.0
  * redis-6.0.6
  * spark-2.1.1 => 3.2.1
  * zookeeper => 3.4.6

* 在IJ中将全局库中的scala-sdk更新到2.12.15