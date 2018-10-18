package spark.configuration

trait FormatConstants
case object CSV extends FormatConstants
case object PARQUET extends FormatConstants
case object AVRO extends FormatConstants {
  val formatString = "com.databricks.spark.avro"
}
case object JSON extends FormatConstants
