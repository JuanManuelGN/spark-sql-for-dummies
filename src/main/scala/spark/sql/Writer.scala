package spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.configuration.{CSV, FormatTarget, PARQUET}

trait Writer {
  def write(df: DataFrame , config: FormatTarget)(implicit spark: SparkSession) = {
    val outputPath = config.outputPath
    val outputFormat = config.outputFormat
    outputFormat match {
      case CSV => df.write.csv(outputPath)
      case PARQUET => df.write.parquet(outputPath)
    }
  }
}
