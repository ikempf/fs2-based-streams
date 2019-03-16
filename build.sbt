lazy val `fs2-queue-streams` = (project in file("."))
  .settings(
    organization := "com.ikempf",
    name := "fs2-queue-streams",
    scalaVersion := "2.12.8",
    scalacOptions += "-Ypartial-unification",
    libraryDependencies ++= List(
      "org.typelevel" %% "cats-core" % "1.6.0",
      "org.typelevel" %% "cats-effect" % "1.2.0",
      "co.fs2" %% "fs2-core" % "1.0.4",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8"),
    scalacOptions ++= List(
      "-target:jvm-1.8",
      "-feature",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-deprecation",
      "-language:higherKinds",
      "-Xlint",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-unused:implicits",
      "-Ywarn-unused:imports",
      "-Ywarn-unused:locals",
      "-Ywarn-unused:params",
      "-Ywarn-unused:patvars",
      "-Ywarn-unused:privates",
      "-Ywarn-value-discard"
    ),
  )
