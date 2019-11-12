lazy val root = (project in file("."))
  .enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging)
  .settings(
    libraryDependencies ++= Seq(
      "org.scalikejdbc" %% "scalikejdbc" % "3.4.0",
      "ch.qos.logback" % "logback-classic" % "1.1.2",
      "org.xerial" % "sqlite-jdbc" % "3.7.2",
      "joda-time" % "joda-time" % "2.3",
      "org.joda" % "joda-convert" % "1.6",
      "commons-io" % "commons-io" % "2.4",
      "nu.validator.htmlparser" % "htmlparser" % "1.4",
      "org.seleniumhq.selenium" % "selenium-java" % "2.41.0",
      "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
      "io.github.scala-hamsters" %% "hamsters" % "2.6.0",
      "commons-lang" % "commons-lang" % "2.6"
    ),
    assemblyJarName in assembly := "netkeiba-scraper.jar",
    imageNames in docker := Seq(
      ImageName(
        repository = "netkeiba-scraper",
        tag = Some("latest")
      )
    ),
    dockerBaseImage := "openjdk:9",
    dockerBuildOptions += "--no-cache",
    dockerfile in docker := {
      val stageDir: File = UniversalPlugin.autoImport.stage.value
      val targetDir = "/app/netkeiba-scraper"
      new Dockerfile {
        from("openjdk:9")
        copy(stageDir, targetDir)
        copy(assembly.value, targetDir)
        entryPoint(s"$targetDir/bin/${executableScriptName.value}")
      }
    }
  )
