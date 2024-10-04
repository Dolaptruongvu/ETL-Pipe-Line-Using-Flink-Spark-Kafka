import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.util.Collector
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties
import java.util.UUID
import scala.io.Source
import java.time.Instant

object ReadFilesInPartition {
  def main(args: Array[String]): Unit = {
    // create flink Environtment and kafka setting up
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "kafka:9092")

    val kafkaProducer = new FlinkKafkaProducer[String](
      "partition-topic",
      new SimpleStringSchema(),
      kafkaProps
    )
    
    // get specific bucket from s3
    val path = "s3a://mybucket/partition1"
    val pathObj = new Path(path)
    val fs = FileSystem.get(pathObj.toUri)
    // get all status of file in partition1 folder
    val fileStatusArray = fs.listStatus(pathObj)
    // get only path of each status ( ex : s3a://mybucket/partition1/part-00195-401a90dd-e6dc-4af7-b245-c094f4acff9f-c000.csv )
    val filePaths: Array[String] = fileStatusArray.map(_.getPath.toString)
    // convert to elements and put to stream
    val fileStream: DataStream[String] = env.fromElements(filePaths: _*)
    val uuidOfAllParts = UUID.randomUUID().toString
    // read all files and create json message
    val jsonFile = fileStream.map(filePath => {
      println(filePath)
      val jsonMessage = createMessage(filePath,uuidOfAllParts)
      jsonMessage
    })

    jsonFile.addSink(kafkaProducer)
    env.execute("tessttt")

}
// read file and return content
def readFileString(filePath:String): String ={
  val objPath = new Path(filePath)
  val fs = FileSystem.get(objPath.toUri)
  val inputStream = fs.open(objPath)
  val content = Source.fromInputStream(inputStream).mkString
  inputStream.close()

  content
}
// create json message based on filePath
def createMessage(filePath: String,uuidOfAllParts :String): String = {
    val getCsvName = filePath.split("/").last
    val partNo = getCsvName.split("-")(1)
    val content = readFileString(filePath)
    val contentSize = content.getBytes.length
    val uuidPart: String = UUID.randomUUID().toString
    s"""{
      "id": "$uuidOfAllParts",
      "display_name": "large_file_100MB",
      "create_date": "${Instant.now()}",
      "fields": {
        "mimetype": [
          "text/csv"
        ],
        "content": [
          "$content"
        ],
        "filename": [
          "$getCsvName"
        ],
        "partno": [
          "$partNo"
        ],
        "partid": [
          "$uuidPart"
        ],
        "partsize": [
          "$contentSize"
        ],
        "origfileid": [
          "original file uuid"
        ]
      }
    }"""
  }

}
