package spark.configuration

trait FormatTransformerConfiguration
case class FormatTransformerConfig(source:FormatSource,
                                   target: FormatTarget) extends FormatTransformerConfiguration
case class FormatSource(inputPath: String,
                        inputFormat: FormatConstants,
                        options: Option[Map[String,String]] = None)
case class FormatTarget(outputPath: String,
                        outputFormat: FormatConstants,
                        options: Option[Map[String, String]] = None)
