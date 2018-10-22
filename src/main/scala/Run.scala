import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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

//    // Parquet to Parquet
//    val sourceParquet = FormatSource("data/7e4355de3f504aab-35a9cdb0ec71d8bd_1817819498_data.0.parq", PARQUET)
//    val targetParquet = FormatTarget("out/parquet", PARQUET)
//    run(sourceParquet,targetParquet)
//
//    // Avro to Avro
//    val source = FormatSource("data/part-00001-584e5ca5-c3fc-49b6-be4f-1b31a8149574.c000.avro", AVRO)
//    val target = FormatTarget("out/avro/avro", AVRO)
//    run(source,target)
//
//    // Avro to Json
//    val sourceEpisodes = FormatSource("data/episodes.avro", AVRO)
//    val targetEpisodesJson = FormatTarget("out/json", JSON)
//    run(sourceEpisodes,targetEpisodesJson)
//
//    // Avro to CSV
//    val targetEpisodesCSV = FormatTarget("out/csv/csv", CSV)
//    run(sourceEpisodes,targetEpisodesCSV)
//
//    // Avro to CSV with delimiter custom
//    val targetEpisodesCSVcustomDelimiter = FormatTarget("out/csv/customDelimiter", CSV,Some(Map("delimiter"->"|")))
//    run(sourceEpisodes,targetEpisodesCSVcustomDelimiter)
//
//    // Class to Avro
//    val targetUser = FormatTarget("out/avro/users",AVRO)
//    val dfUser = spark.createDataFrame(Seq(User("juan",40,"garcía")))
//    write(dfUser,targetUser)
//
//    loadGZWriteAvroStaticExample
//    loadGZWriteAvroDynamicColumns
//
//    // GZ
//    def loadGZWriteAvroStaticExample = {
//          def parseToPrueba(c:String) = {
//            val lista = c.split("\u0001")
//            Row(lista(0),lista(1),lista(2),lista(3),lista(4),lista(5),lista(6),lista(7),lista(8),lista(9),lista(10))
//          }
//      val gzToRDD: String => RDD[String] = gzPath => spark.sparkContext.textFile(gzPath)
//
//          val rddString: RDD[String] = gzToRDD("data/000000_0.gz")
//          rddString.saveAsTextFile("out/gzip")
//
//          val rddPrueba = rddString.map(l => parseToPrueba(l.toString))
//          rddPrueba.take(10).foreach(println)
//
//          val schemaString = "uno dos tres cuatro cinco seis siete ocho nueve diez once"
//          val fields =
//            schemaString
//              .split(" ")
//              .map(fieldName => StructField(fieldName, StringType, nullable = true))
//          val schema = StructType(fields)
//          val dfGZ = spark.createDataFrame(rddPrueba,schema)
//          dfGZ.printSchema()
//          dfGZ.show()
//      val targetGZ = FormatTarget("out/gzip/static", AVRO)
//          write(dfGZ,targetGZ)
//    }
//    def loadGZWriteAvroDynamicColumns = {
//      /**
//        * Dado un rdd con una única fila se va a construir un esquema
//        * para montar un dataframe
//        */
//      val buildSchema: (RDD[String],String) => StructType =
//        (rdd,delimiter) => {
//          // Obtener el número de campos
//          val columnsNumber = rdd.take(1).mkString.split(delimiter).length
//          val columns: List[String] = (1 to columnsNumber).map(pos => s"column_$pos").toList
//          val fields =
//            columns.map(fieldName => StructField(fieldName, StringType, nullable = true))
//          StructType(fields)
//        }
//      val parseToRow: (String,String) => Row =
//        (line,delimiter) => {
//          val entity = line.split(delimiter)
//          val seq = entity.toSeq
//          Row.fromSeq(seq)
//        }
//      val buildDF: (RDD[String],StructType,String) => DataFrame =
//        (rdd,schema,delimiter) => {
//          val rddRow: RDD[Row] = rdd.map(l => parseToRow(l,delimiter))
//          rddRow.take(10).foreach(println)
//          spark.createDataFrame(rddRow,schema)
//        }
//      val gzToRDD: String => RDD[String] = gzPath => spark.sparkContext.textFile(gzPath)
//      val rddCrucemosLosDedos: RDD[String] = gzToRDD("data/000000_0.gz")
//      val schemaDF = buildSchema(rddCrucemosLosDedos,"\u0001")
//      val dfCrucemosLosDedos = buildDF(rddCrucemosLosDedos,schemaDF,"\u0001")
//      dfCrucemosLosDedos.printSchema()
//      dfCrucemosLosDedos.show()
//      val targetGZ = FormatTarget("out/gzip/dynamicColumns", AVRO)
//      write(dfCrucemosLosDedos,targetGZ)
//    }

    loadGZAndSchemaWriteFile
    // GZ
    /**
      * Con un csv únicamente se puede indicar el nombre de las columnas
      */
    def loadGZAndSchemaWriteFile = {
      val df = load(FormatSource("data/csvHeaderSample.csv",CSVheader,Some(Map("header"->"true"))))
      df.show()
      df.printSchema()
    }
  }
}
