name := "TrainApp"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0" % "provided"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.14"
libraryDependencies += "org.apache.opennlp" % "opennlp-tools" % "1.9.3"
libraryDependencies += "info.picocli" % "picocli" % "4.5.1"
