import pureconfig._
import pureconfig.generic.auto._
object ConfigurationModule {
  final case class ConfigurationError(message: String) extends Exception
  final case class Configuration(appName: String, csvPath: String)
  val configuration: Either[ConfigurationError, Configuration] = ConfigSource.default
    .load[Configuration]
    .fold(
      fa => Left(ConfigurationError(fa.head.description)),
      configuration => Right(configuration)
    )
}
