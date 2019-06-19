resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.21")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.8")
libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.0-pre4"
