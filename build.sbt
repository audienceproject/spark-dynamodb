organization := "com.audienceproject"

name := "spark-dynamodb"

version := "0.1"

description := "Plug-and-play implementation of an Apache Spark custom data source for AWS DynamoDB."

scalaVersion := "2.11.12"

resolvers += "DynamoDBLocal" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.325"
libraryDependencies += "com.amazonaws" % "DynamoDBLocal" % "[1.11,2.0)" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

fork in Test := true
javaOptions in Test ++= Seq("-Djava.library.path=./lib/sqlite4java", "-Daws.dynamodb.endpoint=http://localhost:8000")

/**
  * Maven specific settings for publishing to support Maven native projects.
  */
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
publishTo := version { v: String =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}.value
val publishSnapshot: Command = Command.command("publishSnapshot") { state =>
    val extracted = Project extract state
    import extracted._
    val currentVersion = getOpt(version).get
    val newState = Command.process(s"""set version := "$currentVersion-SNAPSHOT" """, state)
    Project.extract(newState).runTask(PgpKeys.publishSigned in Compile, newState)
    state
}
commands ++= Seq(publishSnapshot)
pomIncludeRepository := { _ => false }
pomExtra := <url>https://github.com/audienceproject/spark-dynamodb</url>
    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>https://opensource.org/licenses/apache-2.0</url>
        </license>
    </licenses>
    <scm>
        <url>git@github.com:audienceproject/spark-dynamodb.git</url>
        <connection>scm:git:git//github.com/audienceproject/spark-dynamodb.git</connection>
        <developerConnection>scm:git:ssh://github.com:audienceproject/spark-dynamodb.git</developerConnection>
    </scm>
    <developers>
        <developer>
            <id>jacobfi</id>
            <name>Jacob Fischer</name>
            <email>jacob.fischer@audienceproject.com</email>
            <organization>AudienceProject</organization>
            <organizationUrl>https://www.audienceproject.com</organizationUrl>
        </developer>
    </developers>