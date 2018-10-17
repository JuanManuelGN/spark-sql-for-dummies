package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Partido(key:String,
                   temporada:String,
                   jornada:String,
                   equipoCasa:String,
                   equipoVisitante:String,
                   golesCasa:String,
                   golesVisitante:String,
                   fecha:String,
                   timestamp:String)
class PoCSparkSQL {
  val spark = SparkSession.builder
    .master("local")
    .appName("Word Count")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val df = spark.read.textFile("data/DataSetPartidos.txt")
  def partidosDF: RDD[Partido] =
    spark.sparkContext
    .textFile("data/DataSetPartidos.txt")
    .map(_.split("::"))
    .map(fields =>
      Partido(
        fields(0),fields(1),fields(2),
        fields(3),fields(4),fields(5),
        fields(6),fields(7),fields(8)))
}
object PoCSparkSQL extends App {
  val poc = new PoCSparkSQL
  poc.df.show()
  val df = poc.spark.createDataFrame(poc.partidosDF)
  df.createOrReplaceTempView("partidos")
  val partidosMalaga = poc.spark.sql("select * from partidos where equipoCasa == 'CD Malaga'")
  partidosMalaga.show()

}
