import modules.ConfigurationModule
import zio.clock.Clock
import zio.random.Random
import zio.system.System
import zio.console.Console
import zio.blocking.Blocking
object ZIOHelpers {
  type AppEnvironment = zio.ZEnv with ConfigurationModule with LoggingModule with AcmeModule
  val liveEnvironments =
    new System.Live with Clock.Live with Console.Live with Blocking.Live
    with ConfigurationModule.Live with Random.Live with LoggingModule.Live with AcmeModule.Live
}
