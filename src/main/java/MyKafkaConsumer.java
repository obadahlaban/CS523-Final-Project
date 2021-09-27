

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

public class MyKafkaConsumer {
	public static void main(String[] args) throws StreamingQueryException {

		Logger.getLogger("org").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder()
				.appName("Spark Kafka Integration Structured Streaming")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> ds = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "soccerTopic")
				.load();

		Dataset<Row> lines = ds.selectExpr("CAST(value AS STRING)");
		
		Dataset<Row> linesExpressons = lines
                .selectExpr("value",
                        "split(value,',')[0] as created_at",
                        "split(value,',')[1] as username",
                        "split(value,',')[2] as screen_name",
                        "split(value,',')[3] as followers_count",
                        "split(value,',')[4] as friends_count",
                        "split(value,',')[5] as favorites_count",
                        "split(value,',')[6] as lang",
                        "split(value,',')[7] as source_data")
                .drop("value");
		
		linesExpressons = regexReplace(linesExpressons);
		linesExpressons = cast(linesExpressons);

		StreamingQuery query = linesExpressons.writeStream().outputMode("append").format("console").start();

		linesExpressons.coalesce(1).writeStream().format("csv").outputMode("append")
		    .trigger(Trigger.ProcessingTime(10))
	        .option("truncate", false)
	        .option("maxRecordsPerFile", 10000)
		    .option("path", "hdfs://localhost:8020/user/cloudera/soccerData")
		    .option("checkpointLocation", "hdfs://localhost:8020/user/cloudera/soccerCheck")
		    .start().awaitTermination();
		
		query.awaitTermination();
	}

	private static Dataset<Row> cast(Dataset<Row> linesExpressons) {
		linesExpressons = linesExpressons
		                    .withColumn("created_at",functions.col("created_at").cast(DataTypes.StringType))
		                    .withColumn("username",functions.col("username").cast(DataTypes.StringType))
		                    .withColumn("screen_name",functions.col("screen_name").cast(DataTypes.StringType))
		                    .withColumn("followers_count",functions.col("followers_count").cast(DataTypes.IntegerType))
		                    .withColumn("friends_count",functions.col("friends_count").cast(DataTypes.IntegerType))
		                    .withColumn("favorites_count",functions.col("favorites_count").cast(DataTypes.IntegerType))
		                    .withColumn("lang",functions.col("lang").cast(DataTypes.StringType))
							.withColumn("source_data",functions.col("source_data").cast(DataTypes.StringType));
		return linesExpressons;
	}

	private static Dataset<Row> regexReplace(Dataset<Row> linesExpressons) {
		linesExpressons = linesExpressons
		                    .withColumn("created_at", functions.regexp_replace(functions.col("created_at"),
		                            " ", ""))
		                    .withColumn("username", functions.regexp_replace(functions.col("username"),
		                            " ", ""))
		                    .withColumn("screen_name", functions.regexp_replace(functions.col("screen_name"),
		                            " ", ""))
		                    .withColumn("followers_count", functions.regexp_replace(functions.col("followers_count"),
		                            " ", ""))
		                    .withColumn("friends_count", functions.regexp_replace(functions.col("friends_count"),
		                            " ", ""))
		                    .withColumn("favorites_count", functions.regexp_replace(functions.col("favorites_count"),
		                            " ", ""))
		                    .withColumn("lang", functions.regexp_replace(functions.col("lang"),
		                            " ", ""))
		                    .withColumn("source_data", functions.regexp_replace(functions.col("source_data"),
		                            " ", ""));
		return linesExpressons;
	}
	

}

