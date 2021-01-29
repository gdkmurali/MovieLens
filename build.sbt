name := "MovieLens"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
                            "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
                            "org.scalatest" %% "scalatest" % "3.0.1" % "test")