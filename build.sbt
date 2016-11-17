lazy val root = (project in file(".")).
  settings(
    organization := "Justontheway",
    name := "SwissArmyKnife",
    version := "1.0",
    scalaVersion := "2.10.5",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.0",
      "org.apache.spark" %% "spark-sql" % "1.6.0",
      "org.apache.spark" %% "spark-hive" % "1.6.0",
      "org.apache.spark" %% "spark-hive-thriftserver" % "1.6.0",
      "org.apache.hbase" % "hbase-common" % "1.2.2",
      "org.apache.hbase" % "hbase-client" % "1.2.2",
      "org.apache.hbase" % "hbase-protocol" % "1.2.2",
      "org.apache.commons" % "commons-pool2" % "2.4.2",
      "com.squareup.okhttp" % "okhttp" % "2.7.5",
      "joda-time" % "joda-time" % "2.7",
      "org.joda" % "joda-convert" % "1.7"
    )
  )
