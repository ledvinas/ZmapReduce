package mapReduceZIO

import com.github.tototoshi.csv.CSVWriter
import zio.{Has, Task, ZIO, ZLayer}

object WriteOutFile {

  // type alias, to use for other layers
  type WriteFilesEnv = Has[WriteOutFile.Service]

  // service definition
  trait Service {
    def writeToFile[B](filePath :String, data: Seq[Map[String, B]]): Task[Unit]
  }

  val live: ZLayer[Any, Nothing, WriteFilesEnv] = ZLayer.succeed {
    new Service {
      override def writeToFile[B](filePath :String, data: Seq[Map[String, B]]): Task[Unit] = Task {

        println(data)
        val writer = CSVWriter.open(filePath)
        writer.writeAll(data.map(_.values.toList))
        writer.close()
      }
    }
  }

  def writeToFile[B](filePath :String, data: Seq[Map[String, B]]): ZIO[WriteFilesEnv, Throwable, Unit] =
    ZIO.serviceWith(_.writeToFile(filePath, data))
}
