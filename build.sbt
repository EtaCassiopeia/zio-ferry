val scalaLts          = "3.3.8" // Libraries compile against the LTS so any Scala 3 consumer can use them
val scalaNext         = "3.9.0"
val zioVersion        = "2.1.27"
val rezilienceVersion = "0.10.5"

inThisBuild(
  List(
    organization       := "io.github.etacassiopeia",
    scalaVersion       := scalaLts,
    crossScalaVersions := Seq(scalaLts, scalaNext),
    description        := "A ZIO 2 micro-batching bulkhead that keeps per-key order",
    homepage           := Some(uri("https://etacassiopeia.github.io/zio-ferry/")),
    licenses           := List("Apache-2.0" -> uri("https://www.apache.org/licenses/LICENSE-2.0")),
    scmInfo            := Some(
      ScmInfo(
        uri("https://github.com/EtaCassiopeia/zio-ferry"),
        "scm:git:https://github.com/EtaCassiopeia/zio-ferry.git"
      )
    ),
    developers := List(
      Developer("EtaCassiopeia", "EtaCassiopeia", "zainalpour@gmail.com", uri("https://github.com/EtaCassiopeia"))
    ),
    // The version comes from the git tag (sbt-dynver, via sbt-ci-release): v0.1.0 publishes 0.1.0
    versionScheme := Some("early-semver")
  )
)

lazy val root = project
  .in(file("."))
  .aggregate(testkit, core, examples)
  .settings(
    name           := "zio-ferry-root",
    publish / skip := true
  )

// Deterministic simulation and TLA+ conformance tooling. Generic: nothing in it knows about Ferry.
lazy val testkit = project
  .in(file("testkit"))
  .settings(
    name           := "zio-ferry-testkit",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val core = project
  .in(file("core"))
  .dependsOn(testkit % Test)
  .settings(
    name := "zio-ferry",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // The API reference published with the documentation site
    Compile / doc / scalacOptions ++= Seq(
      "-social-links:github::https://github.com/EtaCassiopeia/zio-ferry",
      "-source-links:github://EtaCassiopeia/zio-ferry/main",
      "-sourceroot",
      (ThisBuild / baseDirectory).value.getAbsolutePath
    )
  )

lazy val examples = project
  .in(file("examples"))
  .dependsOn(core)
  .settings(
    name                               := "zio-ferry-examples",
    publish / skip                     := true,
    libraryDependencies += "nl.vroste" %% "rezilience" % rezilienceVersion
  )

addCommandAlias("fmt", "scalafmtSbt; scalafmtAll")
addCommandAlias("check", "scalafmtSbtCheck; scalafmtCheckAll")
