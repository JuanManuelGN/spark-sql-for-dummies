package spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.configuration.{AVRO, CSV, FormatSource, PARQUET}
import com.databricks.spark.avro._

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
      case AVRO => {
        spark.read.avro(inputPath)
      }
    }
  }
}
