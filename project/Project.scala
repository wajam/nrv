import sbt._
import Keys._

object NrvBuild extends Build {
	var commonResolvers = Seq(
		"Maven.org" at "http://repo1.maven.org/maven2",
		"Sun Maven2 Repo" at "http://download.java.net/maven/2",
		"Scala-Tools" at "http://scala-tools.org/repo-releases/",
		"Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
		"Oracle Maven2 Repo" at "http://download.oracle.com/maven",
		"Sonatype" at "http://oss.sonatype.org/content/repositories/release"
	)

	var commonDeps = Seq (
    "org.slf4j" % "slf4j-nop" % "1.5.8",
		"io.netty" % "netty" % "3.3.1.Final" withSources(),
		"org.scalatest" %% "scalatest" % "1.7.1" % "test",
		"junit" % "junit" % "4.10" % "test"
	)

	var zookeeperDeps = Seq (
		"org.apache.zookeeper" % "zookeeper" % "3.4.3" exclude("javax.jms", "jms") exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools")
	)

	lazy val root = Project(
		id = "nrv",
		base = file(".")
	) aggregate(core, zookeeper)


  val defaultSettings = Defaults.defaultSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.appaquet",
    version := "0.1-SNAPSHOT"
  )
  

  // all keys at http://harrah.github.com/xsbt/latest/sxr/Keys.scala.html#295872
	lazy val core = Project(	
		id = "nrv-core",
		base = file("nrv-core"),
		settings = defaultSettings ++ Seq(
      // some other
    )
	)

	lazy val zookeeper = Project(
		id = "nrv-zookeeper",
		base = file("nrv-zookeeper"),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++= zookeeperDeps
    )
	) dependsOn(core)
}

