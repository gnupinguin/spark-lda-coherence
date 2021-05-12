name := "LdaCoherence"

version := "1.1"

organization := "io.github.gnupinguin"

scalaVersion := "2.12.13"

publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1" % "provided"

