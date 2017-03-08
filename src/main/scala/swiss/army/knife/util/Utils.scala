package swiss.army.knife.util

object Utils extends Logging {
  /**
   * Get the ClassLoader which loaded this lib.
   */
  def getLibClassLoader: ClassLoader = getClass.getClassLoader

}

