package spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.configuration._

trait Writer {
  def write(df: DataFrame , config: FormatTarget)(implicit spark: SparkSession) = {
    val outputPath = config.outputPath
    val outputFormat = config.outputFormat
    outputFormat match {
      case CSV => {
        val delimiter:String = config.options.map(optionMap => optionMap.apply("delimiter")).getOrElse(",")
        df.write.option("delimiter",delimiter).csv(outputPath)
      }
      case PARQUET => {
//        df.write.parquet(outputPath)
        df.write.format(PARQUET.toString.toLowerCase).mode("error").save(outputPath)
      }
      case AVRO => df.write.format(AVRO.formatString).save(outputPath)
      case JSON => df.write.json(outputPath)
    }
  }
}
