import sbt._

object Dependencies {

  /**
    * Version locks for all libraries that share a version number from their parent project,
    * with akka being a good example.
    */
  object v {
    val sangria       = "1.3.3"
    val akka          = "2.5.12"
    val akkaHttp      = "10.1.0"
    val joda          = "2.9.4"
    val jodaConvert   = "1.7"
    val cuid          = "0.1.1"
    val play          = "2.6.8"
    val scalactic     = "3.0.4"
    val scalaTest     = "3.0.4"
    val slick         = "3.2.3"
    val jackson       = "2.8.4"
    val http4sVersion = "0.18.21"
  }

  val jodaTime    = "joda-time" % "joda-time" % v.joda
  val jodaConvert = "org.joda" % "joda-convert" % v.jodaConvert
  val joda        = Seq(jodaTime, jodaConvert)

  val cuid      = "cool.graph"    % "cuid-java"  % v.cuid
  val scalactic = "org.scalactic" %% "scalactic" % v.scalactic
  val scalaTest = "org.scalatest" %% "scalatest" % v.scalaTest % Test

  val slickCore   = "com.typesafe.slick" %% "slick" % v.slick
  val slickHikari = "com.typesafe.slick" %% "slick-hikaricp" % v.slick
  val slickJoda   = "com.github.tototoshi" %% "slick-joda-mapper" % "2.3.0"
  val slick       = Seq(slickCore, slickHikari, slickJoda)

  val mariaDbClient  = "org.mariadb.jdbc"  % "mariadb-java-client" % "2.1.2"
  val postgresClient = "org.postgresql"    % "postgresql"          % "42.2.2"
  val mongoClient    = "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.0"
  val sqliteClient   = "org.xerial"        % "sqlite-jdbc"         % "3.25.2"

  val playJson    = "com.typesafe.play" %% "play-json"    % v.play
  val playStreams = "com.typesafe.play" %% "play-streams" % v.play

  val nettyTransport = "io.netty" % "netty-transport" % "4.1.17.Final"
  val nettyHandler   = "io.netty" % "netty-handler" % "4.1.17.Final"
  val netty          = Seq(nettyTransport, nettyHandler)

  val akka            = "com.typesafe.akka" %% "akka-actor"        % v.akka
  val akkaStream      = "com.typesafe.akka" %% "akka-stream"       % v.akka
  val akkaTestKit     = "com.typesafe.akka" %% "akka-testkit"      % v.akka
  val akkaHttp        = "com.typesafe.akka" %% "akka-http"         % v.akkaHttp
  val akkaHttpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % v.akkaHttp
  val akkaHttpCors    = "ch.megard"         %% "akka-http-cors"    % "0.3.0"
  val akkaHttpPlayJson = "de.heikoseeberger" %% "akka-http-play-json" % "1.20.0" excludeAll (
    ExclusionRule(organization = "com.typesafe.akka"),
    ExclusionRule(organization = "com.typesafe.play")
  )

  val jsr305        = "com.google.code.findbugs"      % "jsr305"                % "3.0.0"
  val caffeine      = "com.github.ben-manes.caffeine" % "caffeine"              % "2.8.0"
  val guava         = "com.google.guava"              % "guava"                 % "19.0"
  val datadogStatsd = "com.datadoghq"                 % "java-dogstatsd-client" % "2.3"

  val sangriaGraphql  = "org.sangria-graphql" %% "sangria" % v.sangria
  val sangriaRelay    = "org.sangria-graphql" %% "sangria-relay" % v.sangria
  val sangriaPlayJson = "org.sangria-graphql" %% "sangria-play-json" % "1.0.4"
  val sangria         = Seq(sangriaGraphql, sangriaRelay, sangriaPlayJson)

  val bugsnagClient = "com.bugsnag" % "bugsnag"      % "3.0.2"
  val specs2        = "org.specs2"  %% "specs2-core" % "3.8.8" % "test"

  val jacksonCore           = "com.fasterxml.jackson.core" % "jackson-core" % v.jackson
  val jacksonDatabind       = "com.fasterxml.jackson.core" % "jackson-databind" % v.jackson
  val jacksonAnnotation     = "com.fasterxml.jackson.core" % "jackson-annotations" % v.jackson
  val jacksonDataformatCbor = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % v.jackson
  val jackson               = Seq(jacksonCore, jacksonDatabind, jacksonAnnotation, jacksonDataformatCbor)

  val enumeratum        = "com.beachape"               %% "enumeratum"                    % "1.5.13"
  val amqp              = "com.rabbitmq"               % "amqp-client"                    % "4.1.0"
  val java8Compat       = "org.scala-lang.modules"     %% "scala-java8-compat"            % "0.8.0"
  val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging"                 % "3.7.0"
  val log4j             = "log4j"                      % "log4j"                          % "1.2.17"
  val jwt               = "com.pauldijou"              %% "jwt-core"                      % "0.14.1"
  val scalajHttp        = "org.scalaj"                 %% "scalaj-http"                   % "2.3.0"
  val evoInflector      = "org.atteo"                  % "evo-inflector"                  % "1.2"
  val snakeYML          = "org.yaml"                   % "snakeyaml"                      % "1.23"
  val librato           = "com.librato.metrics"        % "librato-java"                   % "2.1.0"
  val jettyServer       = "org.eclipse.jetty"          % "jetty-server"                   % "9.3.0.v20150612"
  val scalaUri          = "io.lemonlabs"               %% "scala-uri"                     % "1.3.1"
  val parserCombinators = "org.scala-lang.modules"     %% "scala-parser-combinators"      % "1.0.4"
  val apacheCommons     = "org.apache.commons"         % "commons-lang3"                  % "3.8.1"
  val microMeter        = "io.micrometer"              % "micrometer-registry-prometheus" % "1.0.6"
  val jooq = Vector(
    "org.jooq" % "jooq"        % "3.11.10",
    "org.jooq" %% "jooq-scala" % "3.11.10"
  )

  val jna              = "net.java.dev.jna"      % "jna"                   % "4.5.2"
  val javaxPersistence = "javax.persistence"     % "javax.persistence-api" % "2.2"
  val registry         = "io.dropwizard.metrics" % "metrics-core"          % "3.2.4"
  val checks           = "io.dropwizard.metrics" % "metrics-healthchecks"  % "3.2.4"

  val http4s = Seq(
    "org.http4s" %% "http4s-blaze-server" % v.http4sVersion,
    "org.http4s" %% "http4s-blaze-client" % v.http4sVersion,
    "org.http4s" %% "http4s-circe"        % v.http4sVersion,
    "org.http4s" %% "http4s-dsl"          % v.http4sVersion
  )

  val ujson = Seq(
    "com.lihaoyi" %% "ujson-circe" % "0.6.6",
    "com.lihaoyi" %% "ujson-play"  % "0.6.6"
  )

  lazy val common: Seq[ModuleID] = Seq(
    java8Compat,
    scalaTest,
    scalactic,
    enumeratum,
    caffeine
  )

  lazy val commonServerDependencies: Seq[ModuleID] = common ++ sangria ++ joda ++ Seq(
    guava,
    akkaTestKit,
    akkaHttp,
    akkaHttpPlayJson,
    akkaHttpCors,
    akkaHttpTestKit,
    playJson,
    scalajHttp,
    scalaLogging,
    evoInflector,
    apacheCommons,
    cuid,
    jwt,
    log4j,
    javaxPersistence
  )
}
