import org.apache.avro.Schema
import spark.configuration._
import spark.model.User
import spark.sql.{Loader, Writer}

import scala.reflect.io.Path
import scala.util.Try


object Run extends Loader with Writer with SparkConfig {

  def run(source: FormatSource, target: FormatTarget) = {
    val df = load(source)
    df.printSchema()
    df.show()
    write(df, target)
  }
  def deleteOutDirectory = {
    val path: Path = Path ("out")
    Try(path.deleteRecursively())
  }

  def main(args: Array[String]): Unit = {


    deleteOutDirectory

    // Parquet to Parquet
    val sourceParquet = FormatSource("data/7e4355de3f504aab-35a9cdb0ec71d8bd_1817819498_data.0.parq", PARQUET)
    val targetParquet = FormatTarget("out/parquet", PARQUET)
    run(sourceParquet,targetParquet)

    // Avro to Avro
    val source = FormatSource("data/part-00001-584e5ca5-c3fc-49b6-be4f-1b31a8149574.c000.avro", AVRO)
    val target = FormatTarget("out/avro/avro", AVRO)
    run(source,target)

    // Avro to Json
    val sourceEpisodes = FormatSource("data/episodes.avro", AVRO)
    val targetEpisodesJson = FormatTarget("out/json", JSON)
    run(sourceEpisodes,targetEpisodesJson)

    // Avro to CSV
    val targetEpisodesCSV = FormatTarget("out/csv/csv", CSV)
    run(sourceEpisodes,targetEpisodesCSV)
    // Avro to CSV with delimiter custom
    val targetEpisodesCSVcustomDelimiter = FormatTarget("out/csv/customDelimiter", CSV,Some(Map("delimiter"->"|")))
    run(sourceEpisodes,targetEpisodesCSVcustomDelimiter)

    // Class to Avro
    val targetUser = FormatTarget("out/avro/users",AVRO)
    val dfUser = spark.createDataFrame(Seq(User("juan",40,"garcía")))
    write(dfUser,targetUser)
  }
}
