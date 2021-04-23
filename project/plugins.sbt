credentials += Credentials(
     "GitHub Package Registry", 
     "maven.pkg.github.com", 
     sys.env.get("GITHUB_ACTOR").getOrElse(sys.error("Please define GITHUB_ACTOR")), 
     sys.env.get("GITHUB_TOKEN").getOrElse(sys.error("Please define GITHUB_TOKEN")))

resolvers += "GitHub Package Registry" at "https://maven.pkg.github.com/precog/_"

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.precog" % "sbt-precog" % "3.0.1")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.7")
