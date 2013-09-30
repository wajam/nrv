import sbt._
import Keys._
import com.typesafe.sbt.SbtStartScript

object NrvBuild extends Build {
  val PROJECT_NAME = "nrv"

  var commonResolvers = Seq(
    "Wajam" at "http://ci1.cx.wajam/",
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Twitter" at "http://maven.twttr.com/",
    "Scallop" at "http://mvnrepository.com/"
  )

  var commonDeps = Seq(
    "com.wajam" %% "commons-core" % "0.1-SNAPSHOT",
    "org.slf4j" % "slf4j-api" % "1.6.4",
    "nl.grons" %% "metrics-scala" % "2.2.0" exclude("org.slf4j", "slf4j-api"),
    "io.netty" % "netty" % "3.5.0.Final",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it",
    "com.google.protobuf" % "protobuf-java" % "2.4.1",
    "com.google.guava" % "guava" % "12.0",
    "com.twitter" %% "util-core" % "6.1.0",
    "org.scala-lang" % "scala-actors" % "2.10.2"
  )

  var zookeeperDeps = Seq(
    "org.apache.zookeeper" % "zookeeper" % "3.4.3-cdh4.1.1" exclude("javax.jms", "jms") exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools"),
    "org.rogach" %% "scallop" % "0.9.1"
  )

  var scribeDeps = Seq(
    "org.apache.thrift" % "libthrift" % "0.6.1" exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.thrift" % "libfb303" % "0.6.1"
  )

  var extDeps = Seq(
    "net.liftweb" %% "lift-json" % "2.5-RC4",
    "org.scalatest" %% "scalatest" % "1.9.1",
    "junit" % "junit" % "4.10",
    "org.mockito" % "mockito-core" % "1.9.0"
  )

  val microbenchmarksDeps = Seq(
    "com.google.caliper" % "caliper" % "0.5-rc1"
  )

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.2",
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")
  )

  lazy val root = Project(PROJECT_NAME, file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .aggregate(core)
    .aggregate(ext)
    .aggregate(zookeeper)
    .aggregate(scribe)
    .aggregate(microbenchmarks)

  lazy val core = Project(PROJECT_NAME + "-core", file(PROJECT_NAME + "-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    // See http://code.google.com/p/protobuf/issues/detail?id=368
    // We don't publish docs because it doesn't work with java generated by protobuf
    .settings(publishArtifact in packageDoc := false)
    .settings(testOptions in Test += Tests.Setup(() => System.setProperty("actors.enableForkJoin", "false")) )
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))

  lazy val ext = Project(PROJECT_NAME + "-extension", file(PROJECT_NAME + "-extension"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= extDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .dependsOn(core)

  lazy val zookeeper = Project(PROJECT_NAME + "-zookeeper", file(PROJECT_NAME + "-zookeeper"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= zookeeperDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .dependsOn(core)

  lazy val scribe = Project(PROJECT_NAME + "-scribe", file(PROJECT_NAME + "-scribe"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    // We don't publish docs because it doesn't work with java generated by thrift
    .settings(publishArtifact in packageDoc := false)
    .settings(libraryDependencies ++= scribeDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)

  lazy val microbenchmarks = Project(PROJECT_NAME + "-microbenchmarks", file(PROJECT_NAME + "-microbenchmarks"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= microbenchmarksDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .dependsOn(ext)

  import sbtprotobuf.{ProtobufPlugin => PB}

  val protobufSettings = PB.protobufSettings ++ Seq(
    javaSource in PB.protobufConfig <<= (sourceDirectory in Compile)(_ / "java")
  )

  // We keep it as a separate projet, to avoid a dependency on protoc
  // The protobuf file are under version control, so no need to generate them everytime.
  // To generate them run sbt shell them run ";project nrv-protobuf ;protobuf:generate"
  lazy val protobuf = Project(PROJECT_NAME +  "-protobuf", file("nrv-core"))
    .settings(protobufSettings: _*)
}

