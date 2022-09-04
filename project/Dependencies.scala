import sbt._

object Dependencies {
//  val kindProjector = "org.typelevel"     %% "kind-projector"  % "0.13.1" cross CrossVersion.full
  // Versions
  lazy val sparkVersion = "3.2.1"

  val deps: Seq[ModuleID] = Seq(
    "org.apache.spark"       %% "spark-sql"                        % sparkVersion             % "provided",
    "org.apache.spark"       %% "spark-core"                       % sparkVersion             % "provided",
    "org.apache.spark"       %% "spark-core"                       % sparkVersion             % "provided",
    "io.delta" %% "delta-core" % "2.1.0",
    "com.typesafe" % "config" % "1.4.2" % "provided",
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.24.2" % "provided" exclude("scala-library", "org.scala-lang"),
    "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % Test,
    "org.scalatest" %% "scalatest" % "3.2.12" % Test
  )
}
