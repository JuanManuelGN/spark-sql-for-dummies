package spark.configuration

import org.apache.spark.sql.SparkSession

trait SparkConfig {
  implicit lazy val spark: SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLfofDummies")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.parquet.binaryAsString","true")
      .getOrCreate()
    spark
  }
}
