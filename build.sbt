import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/tectonic"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/tectonic"),
  "scm:git@github.com:slamdata/tectonic.git"))

val Fs2Version = "1.0.0"

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

lazy val test = project
  .in(file("test"))
  .dependsOn(core)
  .settings(name := "tectonic-test")
  .settings(
    libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.4")
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

    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % Fs2Version,
      "co.fs2" %% "fs2-io"   % Fs2Version,

      "org.http4s" %% "jawn-fs2" % "0.13.0"))
  .settings(    // magic rewiring so sbt-jmh works sanely
    Jmh / sourceDirectory := (Compile / sourceDirectory).value,
    Jmh / classDirectory := (Compile / classDirectory).value,
    Jmh / dependencyClasspath := (Compile / dependencyClasspath).value,
    Jmh / compile := (Jmh / compile).dependsOn(Compile / compile).value,
    Jmh / run := (Jmh / run).dependsOn(Jmh / compile).evaluated)
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(JmhPlugin)
