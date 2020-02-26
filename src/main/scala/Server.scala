import zio._
import ZIOHelpers._
import modules.ConfigurationModule
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.PlatformLive
import zio.random.Random
import zio.system.System
import AcmeLogger._
object Server extends App {
  val appRunTime
    : Runtime[System.Live with Clock.Live with Console.Live with Blocking.Live with Random.Live] =
    Runtime(liveEnvironments, PlatformLive.Default)

  val services: ZIO[AppEnvironment, Throwable, Unit] = for {
    configuration <- ConfigurationModule.factory.configuration
    _             <- LoggingModule.factory.info(s"Application Name : ${configuration.appName}")
    acmeService   <- AcmeModule.factory.acmeService
    _             <- LoggingModule.factory.info(s"csv path information ${configuration.csvPath}")
    stageList = acmeService.readCSV(configuration.csvPath)
    _ = stageList.foreach(st => {
      AcmeLogger.logger.info("########################################")
      AcmeLogger.logger.info(s"Input Values : $st")
      acmeService.executeProcess(
        st.stagingPath,
        st.rawPath,
        st.masterPath
      )
      AcmeLogger.logger.info("########################################")
    })
    _ <- LoggingModule.factory.info("Process Finished")
  } yield ()

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    services.provide(liveEnvironments).fold(_ => 1, _ => 0)

}
