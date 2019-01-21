import sbt.Keys.{name, scalacOptions}
import sbt._
import SbtUtils._
import Dependencies._

name := "server"

lazy val commonSettings = Seq(
  organization := "com.prisma",
  organizationName := "graphcool",
  scalaVersion := "2.12.3",
  parallelExecution in Test := false,
  publishArtifact in Test := true,
  // We should gradually introduce https://tpolecat.github.io/2014/04/11/scalac-flags.html
  // These needs to separately be configured in Idea
  scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings", "-language:implicitConversions"),
  resolvers ++= Seq(
    "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
  ),
  libraryDependencies := common
)

lazy val commonServerSettings = commonSettings ++ Seq(libraryDependencies ++= commonServerDependencies)
lazy val prerunHookFile = new java.io.File(sys.props("user.dir") + "/prerun_hook.sh")

def commonDockerImageSettings(imageName: String) = commonServerSettings ++ Seq(
  imageNames in docker := Seq(
    ImageName(s"prismagraphql/$imageName:latest")
  ),
  dockerfile in docker := {
    val appDir    = stage.value
    val targetDir = "/app"

    new Dockerfile {
      from("openjdk:8-slim")
      copy(appDir, targetDir)
      copy(prerunHookFile , s"$targetDir/prerun_hook.sh")
      runShell(s"touch", s"$targetDir/start.sh")
      runShell("echo", "#!/usr/bin/env bash", ">>", s"$targetDir/start.sh")
      runShell("echo", "set -e", ">>", s"$targetDir/start.sh")
      runShell("echo", s".$targetDir/prerun_hook.sh", ">>", s"$targetDir/start.sh")
      runShell("echo", s".$targetDir/bin/${executableScriptName.value}", ">>" ,s"$targetDir/start.sh")
      runShell(s"chmod", "+x", s"$targetDir/start.sh")
      env("COMMIT_SHA", sys.env.getOrElse("COMMIT_SHA", sys.error("Env var COMMIT_SHA required but not found.")))
      env("CLUSTER_VERSION", sys.env.getOrElse("CLUSTER_VERSION", sys.error("Env var CLUSTER_VERSION required but not found.")))
      entryPointShell(s"$targetDir/start.sh")
    }
  }
)

javaOptions in Universal ++= Seq("-Dorg.jooq.no-logo=true")

def imageProject(name: String, imageName: String): Project = imageProject(name).enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging).settings(commonDockerImageSettings(imageName): _*)
def imageProject(name: String): Project = Project(id = name, base = file(s"./images/$name"))
def serverProject(name: String): Project = Project(id = name, base = file(s"./servers/$name")).settings(commonServerSettings: _*).dependsOn(scalaUtils).dependsOn(tracing)
def connectorProject(name: String): Project =  Project(id = name, base = file(s"./connectors/$name")).settings(commonSettings: _*).dependsOn(scalaUtils).dependsOn(prismaConfig).dependsOn(tracing)
def integrationTestProject(name: String): Project =  Project(id = name, base = file(s"./integration-tests/$name")).settings(commonSettings: _*)
def libProject(name: String): Project =  Project(id = name, base = file(s"./libs/$name")).settings(commonSettings: _*)
def normalProject(name: String): Project = Project(id = name, base = file(s"./$name")).settings(commonSettings: _*)

// ####################
//       IMAGES
// ####################
lazy val prismaLocal = imageProject("prisma-local", imageName = "prisma").dependsOn(prismaImageShared)
lazy val prismaProd = imageProject("prisma-prod", imageName = "prisma-prod").dependsOn(prismaImageShared)

lazy val prismaImageShared = imageProject("prisma-image-shared")
  .dependsOn(api)
  .dependsOn(deploy)
  .dependsOn(subscriptions)
  .dependsOn(workers)
  .dependsOn(graphQlClient)
  .dependsOn(prismaConfig)
  .dependsOn(allConnectorProjects)
  .dependsOn(sangriaServer)

// ####################
//       SERVERS
// ####################

lazy val deploy = serverProject("deploy")
  .dependsOn(serversShared % "compile->compile;test->test")
  .dependsOn(deployConnector)
  .dependsOn(akkaUtils)
  .dependsOn(metrics)
  .dependsOn(jvmProfiler)
  .dependsOn(messageBus)
  .dependsOn(graphQlClient)
  .dependsOn(sangriaUtils)
  .dependsOn(auth)

lazy val api = serverProject("api")
  .dependsOn(serversShared % "compile->compile;test->test")
  .dependsOn(deploy % "test->test")
  .dependsOn(apiConnector)
  .dependsOn(messageBus)
  .dependsOn(akkaUtils)
  .dependsOn(metrics)
  .dependsOn(jvmProfiler)
  .dependsOn(cache)
  .dependsOn(auth)
  .dependsOn(sangriaUtils)

lazy val subscriptions = serverProject("subscriptions")
  .dependsOn(serversShared % "compile->compile;test->test")
  .dependsOn(api % "compile->compile;test->test")
  .dependsOn(stubServer % "test->test")
  .settings(
    libraryDependencies ++= Seq(playStreams)
  )

lazy val workers = serverProject("workers")
  .dependsOn(stubServer % "test->test")
  .dependsOn(errorReporting)
  .dependsOn(messageBus)
  .dependsOn(scalaUtils)

lazy val serversShared = serverProject("servers-shared").dependsOn(sangriaServer).dependsOn(connectorUtils % "test->test")

// ####################
//       CONNECTORS
// ####################

lazy val connectorUtils = connectorProject("utils").dependsOn(deployConnectorProjects).dependsOn(apiConnectorProjects)
lazy val connectorShared = connectorProject("shared")
  .settings(
    libraryDependencies ++= slick ++ jooq ++ joda
  )

lazy val deployConnector = connectorProject("deploy-connector")
  .dependsOn(sharedModels)
  .dependsOn(metrics)

lazy val deployConnectorJdbc = connectorProject("deploy-connector-jdbc")
  .dependsOn(deployConnector)
  .dependsOn(connectorShared)

lazy val deployConnectorMySql = connectorProject("deploy-connector-mysql")
  .dependsOn(deployConnectorJdbc)
  .settings(
    libraryDependencies ++= Seq(mariaDbClient)
  )

lazy val deployConnectorPostgres = connectorProject("deploy-connector-postgres")
  .dependsOn(deployConnectorJdbc)
  .settings(
    libraryDependencies ++= Seq(postgresClient)
  )

lazy val deployConnectorMongo = connectorProject("deploy-connector-mongo")
  .dependsOn(deployConnector)
  .dependsOn(mongoUtils)
  .settings(
    libraryDependencies ++= Seq(mongoClient)
  )

lazy val apiConnector = connectorProject("api-connector")
  .dependsOn(sharedModels)
  .dependsOn(gcValues)
  .settings(
    libraryDependencies ++= Seq(apacheCommons)
  )

lazy val apiConnectorJdbc = connectorProject("api-connector-jdbc")
  .dependsOn(apiConnector)
  .dependsOn(metrics)
  .dependsOn(slickUtils)
  .dependsOn(connectorShared)
  .settings(
    libraryDependencies ++= Seq(postgresClient)
  )

lazy val apiConnectorMySql = connectorProject("api-connector-mysql")
  .dependsOn(apiConnectorJdbc)
  .settings(
    libraryDependencies ++= Seq(mariaDbClient)
  )

lazy val apiConnectorPostgres = connectorProject("api-connector-postgres")
  .dependsOn(apiConnectorJdbc)


lazy val apiConnectorMongo = connectorProject("api-connector-mongo")
  .dependsOn(apiConnector)
  .settings(libraryDependencies ++= Seq(mongoClient),
    scalacOptions := {
      val oldOptions = scalacOptions.value
      oldOptions.filterNot(_ == "-Xfatal-warnings")
    })



// ####################
//       SHARED
// ####################

lazy val sharedModels = normalProject("shared-models")
  .dependsOn(gcValues)
  .dependsOn(jsonUtils)
  .dependsOn(scalaUtils)
  .settings(
  libraryDependencies ++= Seq(
    cuid
  ) ++ joda
)

// ####################
//   INTEGRATION TESTS
// ####################

lazy val integrationTestsMySql = integrationTestProject("integration-tests-mysql")
  .dependsOn(deploy % "compile->compile;test->test")
  .dependsOn(api % "compile->compile;test->test")

// ####################
//       LIBS
// ####################

lazy val gcValues = libProject("gc-values")
  .settings(libraryDependencies ++= Seq(
    playJson,
    cuid,
  ) ++ joda)

lazy val akkaUtils = libProject("akka-utils")
  .dependsOn(stubServer % "test->test")
  .dependsOn(errorReporting)
  .dependsOn(scalaUtils)
  .settings(libraryDependencies ++= Seq(
    akkaStream,
    akkaHttp,
    akkaTestKit,
    finagle,
    akkaHttpCors,
    playJson,
    specs2,
    caffeine
  ))
  .settings(
    scalacOptions := {
      val oldOptions = scalacOptions.value
      oldOptions.filterNot(_ == "-Xfatal-warnings")
    })

lazy val metrics = libProject("metrics")
  .dependsOn(errorReporting)
  .dependsOn(akkaUtils)
  .settings(
    libraryDependencies ++= Seq(
      microMeter,
    )
  )

lazy val tracing = libProject("tracing")

lazy val rabbitProcessor = libProject("rabbit-processor")
  .settings(
    libraryDependencies ++= Seq(
      amqp
    ) ++ jackson
  )
  .dependsOn(errorReporting)

lazy val messageBus = libProject("message-bus")
  .dependsOn(errorReporting)
  .dependsOn(akkaUtils)
  .dependsOn(rabbitProcessor)
  .settings(libraryDependencies ++= Seq(
    akka,
    specs2,
    akkaTestKit,
    playJson
  ))


lazy val jvmProfiler = libProject("jvm-profiler")
  .settings(commonSettings: _*)
  .dependsOn(metrics)

lazy val graphQlClient = libProject("graphql-client")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    playJson,
    akkaStream
  ))
  .dependsOn(stubServer % "test->test")
  .dependsOn(akkaUtils)


lazy val stubServer = libProject("stub-server")
    .settings(
      libraryDependencies ++= Seq(
        jettyServer,
        scalaUri,
        parserCombinators,
        scalajHttp,
        specs2
      )
    )

lazy val scalaUtils = libProject("scala-utils")


lazy val errorReporting = libProject("error-reporting")
    .settings(libraryDependencies ++= Seq(
      bugsnagClient,
      playJson
    ))

lazy val sangriaUtils = libProject("sangria-utils")
    .dependsOn(errorReporting)
    .settings(libraryDependencies ++= Seq(
      akkaHttp,
      akkaStream
    ) ++ sangria)

lazy val jsonUtils = libProject("json-utils")
    .settings(libraryDependencies ++= Seq(
      playJson
    ) ++ joda)

lazy val cache = libProject("cache")
    .settings(libraryDependencies ++= Seq(
      caffeine,
      jsr305
    ))

lazy val auth = libProject("auth").settings(libraryDependencies ++= Seq(jwt))

lazy val slickUtils = libProject("slick-utils").settings(libraryDependencies ++= slick)

lazy val prismaConfig = libProject("prisma-config").settings(libraryDependencies ++= Seq(snakeYML, scalaUri))

lazy val mongoUtils = libProject("mongo-utils").settings(libraryDependencies ++= Seq(mongoClient)).dependsOn(jsonUtils)

lazy val sangriaServer = libProject("sangria-server")
  .dependsOn(sangriaUtils)
  .dependsOn(scalaUtils)
  .settings(libraryDependencies ++= Seq(
    akkaHttpPlayJson,
    cuid,
    scalajHttp % Test,
    akkaHttpCors
  ) ++ http4s ++ ujson)

val allDockerImageProjects = List(
  prismaLocal,
  prismaProd
)

val allServerProjects = List(
  api,
  deploy,
  subscriptions,
  workers,
  serversShared,
  sharedModels
)

lazy val deployConnectorProjects = List(
  deployConnector,
  deployConnectorJdbc,
  deployConnectorMySql,
  deployConnectorPostgres,
  deployConnectorMongo
)

lazy val apiConnectorProjects = List(
  apiConnector,
  apiConnectorJdbc,
  apiConnectorMySql,
  apiConnectorPostgres,
  apiConnectorMongo
)

lazy val allConnectorProjects = deployConnectorProjects ++ apiConnectorProjects ++ Seq(connectorUtils, connectorShared)

val allLibProjects = List(
  akkaUtils,
  metrics,
  rabbitProcessor,
  messageBus,
  jvmProfiler,
  graphQlClient,
  stubServer,
  scalaUtils,
  jsonUtils,
  cache,
  errorReporting,
  sangriaUtils,
  prismaConfig,
  mongoUtils
)

val allIntegrationTestProjects = List(
  integrationTestsMySql
)

lazy val images = (project in file("images")).dependsOn(allDockerImageProjects)
lazy val servers = (project in file("servers")).dependsOn(allServerProjects)
lazy val connectors = (project in file("connectors")).dependsOn(allConnectorProjects)
lazy val integrationTests = (project in file("integration-tests")).dependsOn(allIntegrationTestProjects)
lazy val libs = (project in file("libs")).dependsOn(allLibProjects).aggregate(allLibProjects.map(Project.projectToRef): _*)

lazy val root = (project in file("."))
  .aggregate((allServerProjects ++ allDockerImageProjects ++ allConnectorProjects ++ allIntegrationTestProjects).map(Project.projectToRef): _*)
  .settings(
    publish := { } // do not publish a JAR for the root project
  )

