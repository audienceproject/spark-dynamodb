name := "spark-dynamodb"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "DynamoDBLocal" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.325"
libraryDependencies += "com.amazonaws" % "DynamoDBLocal" % "[1.11,2.0)" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
