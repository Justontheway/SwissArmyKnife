package swiss.army.knife.util

/**
 * Various utility methods used by the lib.
 */
object Utils extends Logging {
  /**
   * Get the ClassLoader which loaded this lib.
   */
  def getLibClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Configure a new log4j level
   */
  def setLogLevel(l: org.apache.log4j.Level) {
    org.apache.log4j.Logger.getRootLogger().setLevel(l)
  }

  /**
   * Control our logLevel. This overrides any user-defined log settings.
   * @param logLevel The desired log level as a string.
   * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String) {
    val validLevels = Seq("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")
    if (!validLevels.contains(logLevel)) {
      throw new IllegalArgumentException(
        s"Supplied level $logLevel did not match one of: ${validLevels.mkString(",")}")
    }
    setLogLevel(org.apache.log4j.Level.toLevel(logLevel))
  }

}

