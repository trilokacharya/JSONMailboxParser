name := "Scaling Play"

version := "SNAPSHOT-0.1"

scalaVersion := "2.9.2"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

resolvers += "repo.codehale.com" at "http://repo.codahale.com"

libraryDependencies ++= Seq(
  "com.twitter" % "scalding-core_2.9.2" % "0.8.8",
   "org.apache.hadoop" % "hadoop-core" % "1.0.0",
   "org.json4s" %% "json4s-native" % "3.2.7"
  )
