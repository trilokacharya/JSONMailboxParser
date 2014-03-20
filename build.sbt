name := "Scaling Play"

version := "SNAPSHOT-0.1"

scalaVersion := "2.9.2"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

resolvers += "repo.codehale.com" at "http://repo.codahale.com"

libraryDependencies ++= Seq(
  "com.twitter" % "scalding-core_2.9.2" % "0.8.8",
   "org.apache.hadoop" % "hadoop-core" % "1.0.0",
   "org.json4s" %% "json4s-native" % "3.2.7",
  "joda-time" % "joda-time" % "2.0",
  "org.joda" % "joda-convert" % "1.6",
  "org.slf4j" % "slf4j-simple" % "1.6.4",
  "log4j" % "log4j" % "1.2.14"
)
