object Server extends App {
  override def main(args: Array[String]): Unit =
    AcmeModule.showInformation("C:\\Users\\Indra\\Documents\\ingestion\\path.csv")

  /*    val configuration = ConfigurationModule.configuration
    configuration match {
      case Left(ex)             => AcmeLogger.logger.info("Error on Configuration")
      case Right(configuration) => AcmeLogger.logger.info(s"Hello ${configuration.appName}")
    }*/

}
