import spark.configuration.{FormatSource, FormatTarget, PARQUET, SparkConfig}
import spark.sql.{Loader, Writer}


object Run extends App with Loader with Writer with SparkConfig {
//  val source = FormatSource("data/654e36e4a4ccff1a-b06348403ce8e98_686626557_data.0.parq",PARQUET)
//val source = FormatSource("data/7e4355de3f504aab-35a9cdb0ec71d8bd_1817819498_data.0.parq",PARQUET)
  val source = FormatSource("data/654e36e4a4ccff1a-b06348403ce8e98_68662655_data.0.parq",PARQUET)
  val target = FormatTarget("out",PARQUET)
  val df = load(source)
  df.printSchema()
  df.columns.foreach(c=>println(c))
  df.show(1000)
  println(df.head())

//  write(load(source),target)

}
