import _root_.spark.configuration._
import _root_.spark.model.User
import _root_.spark.sql.{Loader, Writer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.io.Path
import scala.util.Try

case class Run() extends Loader with Writer with SparkConfig {
  def run(source: FormatSource, target: FormatTarget) = {
    val df = load(source)
    df.printSchema()
    df.show()
    write(df, target)
  }

  def deleteOutDirectory = {
    val path: Path = Path("out")
    Try(path.deleteRecursively())
  }

  def parquetToParquet: Unit = {
    val sourceParquet = FormatSource("data/7e4355de3f504aab-35a9cdb0ec71d8bd_1817819498_data.0.parq", PARQUET)
    val targetParquet = FormatTarget("out/parquet", PARQUET)
    run(sourceParquet, targetParquet)
  }

  def avroToAvro: Unit = {
    val source = FormatSource("data/part-00001-584e5ca5-c3fc-49b6-be4f-1b31a8149574.c000.avro", AVRO)
    val target = FormatTarget("out/avro/avro", AVRO)
    run(source, target)
  }

  def avroToJson: Unit = {
    val sourceEpisodes = FormatSource("data/episodes.avro", AVRO)
    val targetEpisodesJson = FormatTarget("out/json", JSON)
    run(sourceEpisodes, targetEpisodesJson)
  }

  def avroToCsv: Unit = {
    val sourceEpisodes = FormatSource("data/part-00000-c42e1e37-05a7-447a-b22b-bbecbc654acd-c000.avro", AVRO)
    val targetEpisodesCSV = FormatTarget("out/csv/csv", CSV)
    run(sourceEpisodes, targetEpisodesCSV)
  }

  def avroToCsvWithDelimiter: Unit = {
    val sourceEpisodes = FormatSource("data/episodes.avro", AVRO)
    val targetEpisodesCSVcustomDelimiter = FormatTarget("out/csv/customDelimiter", CSV, Some(Map("delimiter" -> "|")))
    run(sourceEpisodes, targetEpisodesCSVcustomDelimiter)
  }

  def classToAvro: Unit = {
    val targetUser = FormatTarget("out/avro/users", AVRO)
    val dfUser = spark.createDataFrame(Seq(User("juan", 40, "garcía")))
    write(dfUser, targetUser)
  }

  /**
    * Con este método se lee un archivo .gz que contiene lo que parece un csv que usa como delimitador la cadena
    * "\u0001"
    * Una vez leído se monta un esquema hardcoder y se escribe el resultado en formato avro
    */
  def loadGZWriteAvroStaticExample = {
    def parseToPrueba(c: String) = {
      val lista = c.split("\u0001")
      Row(lista(0), lista(1), lista(2), lista(3), lista(4), lista(5), lista(6), lista(7), lista(8), lista(9), lista(10))
    }

    val rddString: RDD[String] = gzToRDD("data/000000_0.gz")
    rddString.saveAsTextFile("out/gzip")

    val rddPrueba = rddString.map(l => parseToPrueba(l.toString))
    rddPrueba.take(10).foreach(println)

    val schemaString = "uno dos tres cuatro cinco seis siete ocho nueve diez once"
    val fields =
      schemaString
        .split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val dfGZ = spark.createDataFrame(rddPrueba, schema)
    dfGZ.printSchema()
    dfGZ.show()
    val targetGZ = FormatTarget("out/gzip/static", AVRO)
    write(dfGZ, targetGZ)
  }

  /**
    * Funciones auxiliares
    */
  val gzToRDD: String => RDD[String] = gzPath => spark.sparkContext.textFile(gzPath)
  val parseToRow: (String, String, List[StructField]) => Row =
    (line, delimiter, schema) => {
      val listItemsAndType = line.split(delimiter).toList zip schema
      val entity = listItemsAndType.map(item =>
        item._2.dataType match {
          case IntegerType => item._1.toInt
          case _ => item._1
        })

      val seq = entity.toSeq
      Row.fromSeq(seq)
    }
  val buildDF: (RDD[String], StructType, String) => DataFrame =
    (rdd, schema, delimiter) => {
      val rddRow: RDD[Row] = rdd.map(l => parseToRow(l, delimiter, schema.toList))
      rddRow.take(10).foreach(println)
      spark.createDataFrame(rddRow, schema)
    }
  /**
    * Con este método se lee un archivo .gz que contiene lo que parece un csv que usa como delimitador la cadena
    * "\u0001"
    * Una vez leído se monta un esquema de forma dinámica según el número de columnas de la fuente, esto se hace
    * contando el número de columnas que tenga la fuenta y contruyendo un esquema según dicho número de columnas.
    * el resultado se escribe en formato avro
    */
  def loadGZWriteAvroDynamicColumns = {
    /**
      * Dado un rdd con una única fila se va a construir un esquema
      * para montar un dataframe
      */
    val buildSchema: (RDD[String], String) => StructType =
      (rdd, delimiter) => {
        // Obtener el número de campos
        val columnsNumber = rdd.take(1).mkString.split(delimiter).length
        val columns: List[String] = (1 to columnsNumber).map(pos => s"column_$pos").toList
        val fields =
          columns.map(fieldName => StructField(fieldName, StringType, nullable = true))
        StructType(fields)
      }
    val rddCrucemosLosDedos: RDD[String] = gzToRDD("data/000000_0.gz")
    val schemaDF = buildSchema(rddCrucemosLosDedos, "\u0001")
    val dfCrucemosLosDedos = buildDF(rddCrucemosLosDedos, schemaDF, "\u0001")
    dfCrucemosLosDedos.printSchema()
    dfCrucemosLosDedos.show()
    val targetGZ = FormatTarget("out/gzip/dynamicColumns", AVRO)
    write(dfCrucemosLosDedos, targetGZ)
  }

  /**
    * Con un csv únicamente se puede indicar el nombre de las columnas
    */
  def loadGZWithSchemaColumnWriteFile = {
    val df = load(FormatSource("data/csvHeaderSample.csv", CSVheader, Some(Map("header" -> "true"))))
    df.show()
    df.printSchema()
  }

  def createSchema: StructType = {
    StructType(
      Seq(
        StructField("partition", StringType, nullable = true),
        StructField("leter", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("timeStampInit", TimestampType, nullable = true),
        StructField("prefix", StringType, nullable = true),
        StructField("dateInit", StringType, nullable = true),
        StructField("type", StringType, nullable = true),
        StructField("dateEnd", StringType, nullable = true),
        StructField("timeStampEnd", StringType, nullable = true),
        StructField("code", StringType, nullable = true),
        StructField("militarDate", StringType, nullable = true)))
  }

  def loadGzAndMatchSchema(schemaTarget: StructType): Unit = {
    val rdd: RDD[String] = gzToRDD("data/000000_0.gz")
    val df: DataFrame = buildDF(rdd, schemaTarget, "\u0001")
    df.printSchema()
    df.show()
    val targetGZ = FormatTarget("out/gzip/matchSchema", AVRO)
    write(df, targetGZ)
  }
}

object Run extends Loader with Writer with SparkConfig {
  def main(args: Array[String]): Unit = {
    val run = Run()
    run.deleteOutDirectory
    // Parquet to Parquet
    run.parquetToParquet
    // Avro to Avro
    run.avroToAvro
    // Avro to Json
    run.avroToJson
    // Avro to CSV
    run.avroToCsv
    // Avro to CSV with delimiter custom
    run.avroToCsvWithDelimiter
    // Class to Avro
    run.classToAvro
    // GZ con columnas fijas
    run.loadGZWriteAvroStaticExample
    // GZ con columnas dinámicas
    run.loadGZWriteAvroDynamicColumns
    // GZ con columnas fijas y metadatos sobre el nombre de las columnas
    run.loadGZWithSchemaColumnWriteFile
    // GZ haciendo match de un esquema dado con la fuente
    run.loadGzAndMatchSchema(run.createSchema)
  }
}
