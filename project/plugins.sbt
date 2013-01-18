resolvers += "gseitz@github" at "http://gseitz.github.com/maven/"
resolvers += Classpaths.typesafeResolver

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.2.2")
addSbtPlugin("com.typesafe.startscript" % "xsbt-start-script-plugin" % "0.5.1")