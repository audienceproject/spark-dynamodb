name := "spark-dynamodb"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.325"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
