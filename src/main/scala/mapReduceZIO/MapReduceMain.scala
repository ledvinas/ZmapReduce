package mapReduceZIO

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import mapReduceZIO.WriteOutFile.{WriteFilesEnv, writeToFile}
import mapReduceZIO.ZMapReduce.{ZMapReduceEnv, mapReduce}
import zio._
import zio.console.putStrLn

object MapReduceMain extends App {

  /* Task 1 */
  def mapperClicksByDate(fName: String): Seq[(String, Int)] = {
    CSVReader.open(fName).allWithHeaders()
      .groupBy(h => h("date")).mapValues(_.size).toSeq
  }

  def reducerCLicksByDate(output: List[Seq[(String, Int)]]): Seq[Map[String, String]] = {
    output.flatten.groupBy(_._1).map(t => {
      Map("date" -> t._1, "sum" -> t._2.map(_._2).sum.toString)
    }).toSeq
  }
  /* ------------------------------------------------------------------------------------------------------------ */

  /* Task 2 */
  def mapLtuUsers(file: String): Seq[Map[String, String]] = {
    CSVReader.open(file).allWithHeaders()
  }

  def reducerLtuUserClicks(output:  List[Seq[Map[String, String]]]): Seq[Map[String, String]] = {

    (for {
      o <- output
      ltuUserIds = o.filter(m => m.contains("country")).filter(m =>
        m("country") == "LT").map(_ - ("city", "country")
      )
      ltuUserClicks <- ltuUserIds.map(mUser => {
        output.flatten.filter(m => m.contains("user_id"))
          .filter(m => m("user_id") ==  mUser("id"))
      })
    } yield(ltuUserClicks)).flatten
  }


  //main
  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, ExitCode] = {

    val program = args.head
    val workers = args(1).toInt
    val dataSetDir = args(2)
    val outputFileDir = args(3)

    val mapReduceLayer: ZLayer[Any, Nothing, ZMapReduceEnv with WriteFilesEnv] = ZMapReduce.live ++ WriteOutFile.live

    program match {
      case "TASK_1" => {
        //describe
        val clicksProgram = for {
          mr <- mapReduce[Seq[(String, Int)], Seq[Map[String, String]]](dataSetDir, workers, mapperClicksByDate, reducerCLicksByDate)
          _ <- writeToFile(outputFileDir, mr)
        } yield ()

        //run effect
        clicksProgram.provideLayer(mapReduceLayer).exitCode
      }
      case "TASK_2" => {

        //describe effect
        val programFilteredClicks = for {
          mr <- mapReduce[Seq[Map[String, String]], Seq[Map[String, String]]](dataSetDir, workers, mapLtuUsers, reducerLtuUserClicks)
          _ <- writeToFile(outputFileDir, mr)
        } yield ()

        //run effect
        programFilteredClicks.provideLayer(mapReduceLayer).exitCode
      }
      case _ => {
        ZIO.fail(println("Available options: TASK_1 or TASK_2")).exitCode
      }
    }
  }
}