import java.io.File

import AcmeModule.Service
import zio.ZIO

import scala.util.matching.Regex

trait AcmeModule extends Serializable {
  val acmeModule: Service[Any]
}

object Pattern {
  val stagingRawPattern: Regex = "\\d{8}".r
  val masterPattern: Regex     = "(\\d{1,4}([.\\-])\\d{1,2}([.\\-])\\d{1,4})".r
}
object AcmeModule {
  import Pattern._
  case class Acme() {
    private def readDirectory(path: String): Option[File] = {
      val file = new File(path)
      if (file.isDirectory) Some(file) else None
    }
    private def processDirectory(stage: File, pattern: Regex): List[Option[String]] = {
      val files = stage.listFiles().map(_.getName)
      files.map(name => pattern findFirstIn name).toList
    }
    private def validateDirectories(
      stagingDirectory: List[Option[String]],
      rawDirectory: List[Option[String]],
      masterDirectory: List[Option[String]]
    ): Boolean = {
      val stagingCount = stagingDirectory.count(_.isDefined)
      val rawCount     = rawDirectory.count(_.isDefined)
      val masterCount  = masterDirectory.count(_.isDefined)
      stagingCount == rawCount &&
      rawCount == masterCount
    }

    def readCSV(csvPath: String): List[Stage] = {
      import scala.io._
      val csvFile = Source.fromFile(csvPath)
      val csvContent = csvFile
        .getLines()
        .map { line =>
          val lineContent = line.split(",").map(_.trim)
          Stage(lineContent(0), lineContent(1), lineContent(2))
        }
        .toList
      csvContent
    }

    def executeProcess(stagingPath: String, rawPath: String, masterPath: String): Unit = {
      val stagingDirectory: Option[File] = readDirectory(stagingPath)
      val rawDirectory: Option[File]     = readDirectory(rawPath)
      val masterDirectory: Option[File]  = readDirectory(masterPath)
      val result = for {
        staging <- stagingDirectory
        raw     <- rawDirectory
        master  <- masterDirectory
        resultStaging = processDirectory(staging, stagingRawPattern)
        resultRaw     = processDirectory(raw, stagingRawPattern)
        resultMaster  = processDirectory(master, masterPattern)
      } yield validateDirectories(resultStaging, resultRaw, resultMaster)

      if (result.getOrElse(false)) {
        AcmeLogger.logger.info("Success!! we have the same number of elements")
      } else {
        AcmeLogger.logger.error("An error occurred")
      }
    }
  }

  trait Service[R] extends Serializable {
    def acmeService: ZIO[R, Throwable, Acme]
  }
  trait Live extends AcmeModule {
    override val acmeModule: Service[Any] = new Service[Any] {
      override def acmeService: ZIO[Any, Throwable, Acme] =
        ZIO.succeed(Acme())
    }
  }

  object factory extends Service[AcmeModule] {
    override def acmeService: ZIO[AcmeModule, Throwable, Acme] =
      ZIO.accessM[AcmeModule](_.acmeModule.acmeService)
  }

}
