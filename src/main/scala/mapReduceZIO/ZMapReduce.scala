package mapReduceZIO

import zio._

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter


object ZMapReduce {

  type ZMapReduceEnv = Has[ZMapReduce.Service]

  trait Service {
    def mapReduce[A, B](dir: String,
                        workers: Int,
                        map: String => A,
                        reduce: List[A]  => B): ZIO[Any, Throwable, B]
  }

  val live: ZLayer[Any, Nothing, ZMapReduceEnv]  = ZLayer.succeed{
    new Service {

      private def getListOfPaths(directoryName: String): List[String] = {
        val dirList = Files.list(Paths.get(directoryName))
          .iterator.asScala.toList.map(_.toString)

        val csvFiles = dirList.filter(_.contains(".csv"))

        if(csvFiles.isEmpty) dirList.flatMap(p => {
          Files.list(Paths.get(p))
            .iterator.asScala.toList.map(_.toString)
        }) else csvFiles
      }

      override def mapReduce[A, B](dir: String,
                                   workers: Int,
                                   map: String => A,
                                   reduce: List[A] => B): Task[B] = for {
        files <- ZIO.succeed(getListOfPaths(dir))
        m <- ZIO.foreachParN(workers)(files) { file => ZIO.succeed(map(file)) }
        results = reduce(m)
      } yield (results)
    }
  }

  def mapReduce[A, B](dir: String,
                      workers: Int,
                      map: String => A,
                      reduce: List[A]  => B): ZIO[ZMapReduceEnv, Throwable, B] =
    ZIO.serviceWith(_.mapReduce(dir, workers, map, reduce))
}
