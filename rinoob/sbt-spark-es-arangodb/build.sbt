name := "sbt-skelton"
version := "0.0.1"
organization := "net.juniper"
scalaVersion := "2.11.12"

lazy val sparkVersion = "2.4.4"
lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)


libraryDependencies ++= Seq(
 // "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.12.0" % "test",
  "com.amazonaws" % "aws-java-sdk" % "1.11.513" % "provided",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.513" % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.2" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
  "org.mockito" % "mockito-core" % "2.27.0" % "test",
  "org.json" % "json" % "20180813",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8",
  "com.google.guava" % "guava" % "22.0",
  "org.elasticsearch" % "elasticsearch-hadoop" % "7.3.1",
  "com.arangodb" %% "arangodb-spark-connector" % "1.1.0"

)

libraryDependencies ++= sparkDependencies.map(_% "provided")

// test suite settings
fork in Test := true
// javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests

// testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// JAR file settings

test in assembly := {}
// don't include Scala in the JAR file
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
