import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/tectonic"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/tectonic"),
  "scm:git@github.com:slamdata/tectonic.git"))

val Fs2Version = "1.0.2"

ThisBuild / publishAsOSSProject := true

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core, fs2, test, benchmarks)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "tectonic")
  .settings(libraryDependencies += "org.typelevel" %% "cats-effect" % "1.1.0")
  .enablePlugins(AutomateHeaderPlugin)

lazy val fs2 = project
  .in(file("fs2"))
  .dependsOn(
    core,
    test % "test->test")
  .settings(name := "tectonic-fs2")
  .settings(
    libraryDependencies += "co.fs2" %% "fs2-core" % Fs2Version)
  .enablePlugins(AutomateHeaderPlugin)

lazy val harness = project
  .in(file("harness"))
  .dependsOn(
    fs2)
  .settings(name := "tectonic-harness")
  .settings(noPublishSettings)    // mostly for internal testing
  .settings(
    libraryDependencies += "co.fs2" %% "fs2-io" % Fs2Version)
  .enablePlugins(AutomateHeaderPlugin)

lazy val test = project
  .in(file("test"))
  .dependsOn(core)
  .settings(name := "tectonic-test")
  .settings(
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.3.4",
      "org.scalacheck" %% "scalacheck" % "1.14.0"),

    libraryDependencies += "org.specs2" %% "specs2-scalacheck" % "4.3.4" % Test)
  .enablePlugins(AutomateHeaderPlugin)

lazy val benchmarks = project
  .in(file("benchmarks"))
  .dependsOn(core, fs2)
  .settings(name := "tectonic-benchmarks")
  .settings(noPublishSettings)
  .settings(
    scalacStrictMode := false,
    javaOptions += "-XX:+HeapDumpOnOutOfMemoryError",
    javaOptions += s"-Dproject.resource.dir=${(Compile / resourceDirectory).value}",
    javaOptions += s"-Dproject.managed.resource.dir=${(Jmh / resourceManaged).value}",

    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % Fs2Version,
      "co.fs2" %% "fs2-io"   % Fs2Version,

      "org.http4s" %% "jawn-fs2" % "0.14.2"))
  .settings(    // magic rewiring so sbt-jmh works sanely
    Jmh / sourceDirectory := (Compile / sourceDirectory).value,
    Jmh / classDirectory := (Compile / classDirectory).value,
    Jmh / dependencyClasspath := (Compile / dependencyClasspath).value,
    Jmh / compile := (Jmh / compile).dependsOn(Compile / compile).value,
    Jmh / run := (Jmh / run).dependsOn(Jmh / compile).evaluated)
  .settings(
    Jmh / resourceGenerators += Def.task {
      import scala.sys.process._


      val targetDir = (Jmh / resourceManaged).value
      val targetFile = targetDir / "worldcitiespop.txt"

      if (!targetFile.exists()) {
        s"curl -o $targetDir/worldcitiespop.txt.gz http://www.maxmind.com/download/worldcities/worldcitiespop.txt.gz".!!
        s"gunzip $targetDir/worldcitiespop.txt.gz".!!
      }

      Seq(targetFile)
    }.taskValue)
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(JmhPlugin)
