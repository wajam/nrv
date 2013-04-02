resolvers += "gseitz@github" at "http://gseitz.github.com/maven/"

resolvers += Classpaths.typesafeResolver

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.2.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-start-script" % "0.7.0")
