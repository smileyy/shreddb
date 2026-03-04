
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.7"

lazy val root = (project in file("."))
  .settings(
    name := "shreddb",
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-csv" % "1.14.1",

      "org.junit.jupiter" % "junit-jupiter-api" % "6.0.1" % Test,
      "org.hamcrest" % "hamcrest" % "3.0" % Test
    )

  )
