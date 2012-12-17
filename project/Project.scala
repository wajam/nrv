import sbt._
import Keys._

object NrvBuild extends Build {
  var commonResolvers = Seq(
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Scallop" at "http://mvnrepository.com/"
  )

  var commonDeps = Seq(
    "org.slf4j" % "slf4j-nop" % "1.6.4",
    "com.yammer.metrics" %% "metrics-scala" % "2.1.2",
    "io.netty" % "netty" % "3.5.0.Final" withSources(),
    "org.scalatest" %% "scalatest" % "1.7.1" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it"
  )

  var zookeeperDeps = Seq(
    "org.apache.zookeeper" % "zookeeper" % "3.4.3-cdh4.1.1" exclude("javax.jms", "jms") exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools"),
    "com.google.guava" % "guava" % "12.0",
    "org.rogach" %% "scallop" % "0.6.0"
  )

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT"
  )

  lazy val root = Project("nrv", file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .aggregate(core)
    .aggregate(zookeeper)

  lazy val core = Project("nrv-core", file("nrv-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))

  lazy val zookeeper = Project("nrv-zookeeper", file("nrv-zookeeper"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= zookeeperDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)
}

