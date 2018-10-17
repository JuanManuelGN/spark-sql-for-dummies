package spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import spark.configuration.{CSV, FormatSource, PARQUET}
import org.apache.spark.sql.SQLContext

trait Loader {
  def load(config: FormatSource)(implicit spark: SparkSession): DataFrame = {
    val inputPath = config.inputPath
    val inputFormat = config.inputFormat
    inputFormat match {
      case CSV =>
        spark.read.csv(inputPath)
      case PARQUET => {
        spark.read.parquet(inputPath)
      }
    }
  }
}
