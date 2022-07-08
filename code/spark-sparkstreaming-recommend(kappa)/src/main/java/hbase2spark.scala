import breeze.linalg.Broadcaster
import com.alibaba.fastjson.{JSON, JSONObject}
import kafkaStreaming.streamingCore
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.lang.Thread.sleep
import java.text.SimpleDateFormat
import java.util.Date

class hbase2spark{}

object hbase2spark {
  def getHBaseConfiguration(quorum:String, port:String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)
    conf
  }
  def isEqual(x:(Int,Float), y:Float):List[(Int, Int)] = {
    if (x._2==y)
      List((x._1,1))
    else
      List()
  }
//  def getSc() = {
//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[2]").setAppName("kafkaConsumer")
//    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(30))
//    streamingContext
//  }
  def getStream(sc:StreamingContext) = {
    val kafkaParams = Map[String, Object] (
      "bootstrap.servers" -> "ljj-2019213687-0001:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "Hbase2spark",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "partition.assignment.strategy"->"org.apache.kafka.clients.consumer.RangeAssignor"
    )
    val topics = Array("movie_rating_records")
    val stream = KafkaUtils.createDirectStream[String,String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }
//  sc:SparkContext,
  def batch2feature(sc:SparkContext, sc1:StreamingContext) {
//    val hbaseconf = getHBaseConfiguration("ljj-2019213687-0001","2181")
//    hbaseconf.set(TableInputFormat.INPUT_TABLE,"movie_records")
//    // HBase数据转成RDD
//    val hBaseRDD = sc.newAPIHadoopRDD(hbaseconf,classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result]).cache()

    // RDD数据操作
//    var data = hBaseRDD.map(x => {
//      val result = x._2
//      val key = Bytes.toString(result.getRow)
//      val rating = Bytes.toString(result.getValue("details".getBytes,"rating".getBytes)).toFloat
//      val userId = Bytes.toString(result.getValue("details".getBytes,"userId".getBytes)).toInt
//      val movieId = Bytes.toString(result.getValue("details".getBytes,"movieId".getBytes)).toInt
//      val timestamp = Bytes.toString(result.getValue("details".getBytes,"timestamp".getBytes))
//      (key,userId,movieId,rating,timestamp)
//    })

    println("getting stream data")
    val stream = hbase2spark.getStream(sc1).map(x=>{
      val json: JSONObject = JSON.parseObject(x.value())
      (x.key(),json.get("userId").toString.toInt,json.get("movieId").toString.toInt,json.get("rating").toString.toFloat,json.get("timestamp").toString)
    })

//    var data = sc.makeRDD(Array(("", 0, 0, (0.0).toFloat, "")))

    println("Before union")
//    println(data)
    stream.foreachRDD(data => {
      println(data.count())
//      data = data.union(rdd)

      //统计 a)用户历史正反馈次数
      val counterUserIdPos = data.flatMap(x => isEqual((x._2,x._4),1.0.toFloat))
        .reduceByKey((x,y)=> x+y)
      //统计 b)用户历史负反馈次数
      val counterUserIdNeg = data.flatMap(x => isEqual((x._2,x._4),0.0.toFloat))
        .reduceByKey((x,y)=> x+y)
      //统计 c)电影历史正反馈次数
      val counterMovieIdPos = data.flatMap(x => isEqual((x._3,x._4),1.0.toFloat))
        .reduceByKey((x,y)=> x+y)
      //统计 d)电影历史负反馈次数
      val counterMovieIdNeg = data.flatMap(x => isEqual((x._3,x._4),0.0.toFloat))
        .reduceByKey((x,y)=> x+y)

      //统计 e)用户历史点击该分类比例
      val counterUserId2MovieId = data.filter(x=>x._4==1.0)
        .map(x=>(x._2,x._3))
        .groupByKey()
        .flatMapValues(x=>{
          var sum = 0
          val one_hot: Array[Int] = new Array[Int](19)
          //        val jedisIns = new JedisIns("bd",6379,100000)
          val jedisIns:Jedis = new Jedis("ljj-2019213687-0001",6379,100000)
          jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
          for (record<-x) {
            sum=sum+1
            val genres_list = jedisIns.lrange("movie2genres_movieId_" + record.toString,0,-1)
            val it = genres_list.iterator()
            while (it.hasNext) {
              val genresId = it.next().toInt
              one_hot(genresId) = one_hot(genresId)+1
            }
          }
          jedisIns.close()
          var counter:List[(Int,Float)] = List()
          for (i<-one_hot.indices) {
            if (one_hot(i)>0) counter = counter :+ (i,one_hot(i).toFloat/sum)
          }
          counter
        })

      //统计 协同过滤模型推荐列表
      val ratingUserId5MovieId = data.map(x=>{
        val rating = Rating(x._2.toInt,x._3.toInt,x._4.toDouble)
        rating
      })
      val rank = 2 //设置隐藏因子
      val numIterations = 2 //设置迭代次数
      val model = ALS.train(ratingUserId5MovieId, rank, numIterations, 0.01) //进行模型训练
      val userIds = data.map(x => x._2).distinct().collect()
      //根据已有数据集建立协同过滤模型后用recommendProducts为用户推荐10个电影
      println("Calculating CF for "+userIds.length.toString + " users...")
      val userIdCFMovieId = userIds.map(x => {
        val products = model.recommendProducts(x, 10)
        (x,products)
      })
      println("Calculating done!")

      // 依次输出统计结果
      val jedisIns = new Jedis("ljj-2019213687-0001",6379,100000)
      jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
      userIdCFMovieId.foreach(x=> {
        jedisIns.del(s"userId_perfer_movieId_${x._1}")
        for (i <- 0 until 10) {
          jedisIns.rpush(s"userId_perfer_movieId_${x._1}", x._2(i).product.toString)
        }
      })
      jedisIns.close()

      counterUserIdPos.foreach( x=> {
        val jedisIns = new Jedis("ljj-2019213687-0001",6379,100000)
        jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
        jedisIns.set("batch2feature_userId_rating1_"+x._1.toString, x._2.toString)
        jedisIns.close()
      })
      counterUserIdNeg.foreach( x=> {
        val jedisIns = new Jedis("ljj-2019213687-0001",6379,100000)
        jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
        jedisIns.set("batch2feature_userId_rating0_"+x._1.toString, x._2.toString)
        jedisIns.close()
      })
      counterMovieIdPos.foreach( x=> {
        val jedisIns = new Jedis("ljj-2019213687-0001",6379,100000)
        jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
        jedisIns.set("batch2feature_movieId_rating1_"+x._1.toString, x._2.toString)
        jedisIns.close()
      })
      counterMovieIdNeg.foreach( x=> {
        val jedisIns = new Jedis("ljj-2019213687-0001",6379,100000)
        jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
        jedisIns.set("batch2feature_movieId_rating0_"+x._1.toString, x._2.toString)
        jedisIns.close()
      })
      counterUserId2MovieId.foreach(x=> {
        val jedisIns = new Jedis("ljj-2019213687-0001",6379,100000)
        jedisIns.auth("Kd7Jdddd16@6djie8gce342NWM9znN4$V")
        jedisIns.set(s"batch2feature_userId_to_genresId_${x._1.toString}_${x._2._1}", x._2._2.toString)
        jedisIns.close()
      })
    })
//    println("After union")
//    println(data)



    println("done")


  }
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    date
  }
  def main(args: Array[String]): Unit = {
    // Spark
    //    val jedisIns = new JedisIns("bd",6379,100000)
    //    jedisIns.testJedis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    while (true) {
      println(s"${NowDate()} [INFO] Begin to calculate batch features")
      val sparkConf = new SparkConf().setAppName("HBaseReadTest").setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
      val sc1:StreamingContext = new StreamingContext(sc, Durations.seconds(30))
      batch2feature(sc, sc1)
      sc1.start()
      sc1.awaitTermination()
      sc1.stop()
      println(s"${NowDate()} [INFO] Success!")
    }
  }
}