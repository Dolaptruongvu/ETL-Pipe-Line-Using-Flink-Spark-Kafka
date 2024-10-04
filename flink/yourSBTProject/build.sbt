
// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.17"
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings". Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.13.12"

// It's possible to define many kinds of settings, such as:

name := "partition-project"
organization := "ch.epfl.scala"
version := "1.0"

// Note, it's not required for you to define these three settings. These are
// mostly only necessary if you intend to publish your library's binaries on a
// place like Sonatype.


// Want to use a published library in your project?
// You can define other libraries as dependencies in your build like this:

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always


// build.sbt
libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % "1.14.4",
  "org.apache.flink" %% "flink-streaming-scala" % "1.14.0",
  "org.apache.flink" %  "flink-core" % "1.14.0",
  "org.apache.flink" %% "flink-connector-kafka" % "1.14.0",
  "org.apache.flink" %  "flink-s3-fs-hadoop" % "1.14.4",
  "org.json4s" %% "json4s-native" % "3.6.11",
  "org.apache.hadoop" % "hadoop-common" % "3.2.1",
  "org.apache.hadoop" % "hadoop-client" % "3.2.1",
  "org.apache.flink" % "flink-connector-files" % "1.15.0"
)

import sbtassembly.MergeStrategy

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard // bỏ qua các file META-INF
  case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.first // giữ lại phiên bản đầu tiên của file
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", "common", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("org", "checkerframework", xs @ _*) => MergeStrategy.first
  case _ => MergeStrategy.first // đối với các file khác, chọn phiên bản đầu tiên
}

run / javaOptions ++= Seq("-Xmx8G", "-XX:+UseG1GC")


// Here, `libraryDependencies` is a set of dependencies, and by using `+=`,
// we're adding the scala-parser-combinators dependency to the set of dependencies
// that sbt will go and fetch when it starts up.
// Now, in any Scala file, you can import classes, objects, etc., from
// scala-parser-combinators with a regular import.

// TIP: To find the "dependency" that you need to add to the
// `libraryDependencies` set, which in the above example looks like this:

// "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"

// You can use Scaladex, an index of all known published Scala libraries. There,
// after you find the library you want, you can just copy/paste the dependency
// information that you need into your build file. For example, on the
// scala/scala-parser-combinators Scaladex page,
// https://index.scala-lang.org/scala/scala-parser-combinators, you can copy/paste
// the sbt dependency from the sbt box on the right-hand side of the screen.

// IMPORTANT NOTE: while build files look _kind of_ like regular Scala, it's
// important to note that syntax in *.sbt files doesn't always behave like
// regular Scala. For example, notice in this build file that it's not required
// to put our settings into an enclosing object or class. Always remember that
// sbt is a bit different, semantically, than vanilla Scala.

// ============================================================================

// Most moderately interesting Scala projects don't make use of the very simple
// build file style (called "bare style") used in this build.sbt file. Most
// intermediate Scala projects make use of so-called "multi-project" builds. A
// multi-project build makes it possible to have different folders which sbt can
// be configured differently for. That is, you may wish to have different
// dependencies or different testing frameworks defined for different parts of
// your codebase. Multi-project builds make this possible.

// Here's a quick glimpse of what a multi-project build looks like for this
// build, with only one "subproject" defined, called `root`:

// lazy val root = (project in file(".")).
//   settings(
//     inThisBuild(List(
//       organization := "ch.epfl.scala",
//       scalaVersion := "2.13.12"
//     )),
//     name := "hello-world"
//   )

// To learn more about multi-project builds, head over to the official sbt
// documentation at http://www.scala-sbt.org/documentation.html
