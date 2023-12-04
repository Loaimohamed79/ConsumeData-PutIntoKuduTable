import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder
  .appName("KafkaToKudu")
  .getOrCreate()

val kafkaBootstrapServers = "worker1:9092" 
val kafkaTopic = "test"
val kuduMaster = "worker1:7051"


//small note you must write schemaname.tablename
val kuduTableName = "default.kudu_table"


val schema = StructType(
  Seq(
    StructField("id", IntegerType),
    StructField("age", IntegerType)
  )
)

val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", kafkaTopic)
  .load()

val valueDF = df.selectExpr("CAST(value AS STRING)")
  .withColumn("id", split(col("value"), ",").getItem(0).cast(IntegerType))
  .withColumn("age", split(col("value"), ",").getItem(1).cast(IntegerType))
  .select("id", "age")
  .filter(col("id").isNotNull && col("age").isNotNull)

// Write the DataFrame to the Kudu table
valueDF.writeStream
  .foreachBatch { (batchDF, _) =>
    batchDF.write
      .option("kudu.master", kuduMaster)
      .option("kudu.table", kuduTableName)
      .mode("append")
      .kudu
  }
  .outputMode("append")
  .start()
  .awaitTermination()

