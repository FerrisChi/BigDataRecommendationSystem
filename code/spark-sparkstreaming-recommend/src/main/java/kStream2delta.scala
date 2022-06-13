import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import hbase2spark.NowDate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, DslSymbol, StringToAttributeConversionHelper}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import io.delta.implicits._
import java.lang.Thread.sleep

class kStream2delta {

}

object kStream2delta{
  def main(args:Array[String]) = {
    // Spark
    //    val jedisIns = new JedisIns("bd",6379,100000)
    //    jedisIns.testJedis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    println(s"${NowDate()} [INFO] Begin to calculate batch features")
    val spark: SparkSession = SparkSession.builder()
      .appName("kStream2delta")
      .master("local[2]")
      .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println(spark)

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node001:9092")
      .option("subscribe", "movie_rating_records")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    df.printSchema()
//    一直运行停不了
    val recordStringDF = df.selectExpr("CAST(value AS STRING)")
    val schema: StructType = StructType(List(
      StructField("userId", IntegerType),
      StructField("movieId", IntegerType),
      StructField("rating", DoubleType),
      StructField("timestamp", StringType)
    ))
    println(schema)
    val recordDF = recordStringDF
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    recordDF.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/delta/records/_checkpoints/")
      .toTable("records")
      .awaitTermination()


    println(s"${NowDate()} [INFO] Success!")
    sleep(1000*60*5)

  }
}