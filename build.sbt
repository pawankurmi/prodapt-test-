import sbt.Keys.libraryDependencies
name := "prodapt_test"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= {
  val sparkVer = "2.3.0.2.6.5.115-3"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-streaming" % sparkVer % "provided" withSources()
  )
}
