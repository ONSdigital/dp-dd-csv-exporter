name := """dp-dd-csv-exporter"""

version := "1.0-SNAPSHOT"

lazy val dp_dd_api = (project in file(".")).enablePlugins(PlayJava)

scalaVersion := "2.11.7"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  javaJdbc,
  cache,
  javaWs,
  javaJpa,
  "org.eclipse.persistence" % "eclipselink" % "2.6.2",
  "uk.co.onsdigital.discovery" % "dd-model" % "1.0.0-SNAPSHOT",
  "org.postgresql" % "postgresql" % "9.4.1208.jre7",
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0",
  "org.testng" % "testng" % "6.9.13.6",
  "de.johoop" % "sbt-testng-interface_2.10" % "3.0.0",
  "org.scalatest" %% "scalatest" % "2.2.1",
  "org.scalatestplus" %% "play" % "1.4.0-M4"
)

EclipseKeys.projectFlavor := EclipseProjectFlavor.Java           // Java project. Don't expect Scala IDE
EclipseKeys.createSrc := EclipseCreateSrc.ValueSet(EclipseCreateSrc.ManagedClasses, EclipseCreateSrc.ManagedResources)  // Use .class files instead of generated .scala files for views and routes
EclipseKeys.preTasks := Seq(compile in Compile)

PlayKeys.externalizeResources := false

fork in run := true
