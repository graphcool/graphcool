package com.prisma.deploy.specutils

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.prisma.auth.AuthImpl
import com.prisma.deploy.DeployDependencies
import com.prisma.deploy.connector.DeployConnector
import com.prisma.deploy.connector.mysql.MySqlDeployConnectorImpl
import com.prisma.deploy.migration.validation.SchemaError
import com.prisma.deploy.schema.mutations.{FunctionInput, FunctionValidator}
import com.prisma.deploy.server.DummyClusterAuth
import com.prisma.errors.{BugsnagErrorReporter, ErrorReporter}
import com.prisma.messagebus.pubsub.inmemory.InMemoryAkkaPubSub
import com.prisma.shared.models.Project

case class DeployDependenciesForTest()(implicit val system: ActorSystem, val materializer: ActorMaterializer) extends DeployDependencies {
  override implicit def self: DeployDependencies = this

  implicit val reporter: ErrorReporter    = BugsnagErrorReporter(sys.env.getOrElse("BUGSNAG_API_KEY", ""))
  override lazy val migrator              = TestMigrator(migrationPersistence, deployPersistencePlugin.deployMutactionExecutor)
  override lazy val clusterAuth           = DummyClusterAuth()
  override lazy val invalidationPublisher = InMemoryAkkaPubSub[String]()

  override def apiAuth = AuthImpl

  override def deployPersistencePlugin: DeployConnector = {
    import slick.jdbc.MySQLProfile.api._
    val sqlInternalHost     = sys.env("SQL_CLIENT_HOST")
    val sqlInternalPort     = sys.env("SQL_CLIENT_PORT")
    val sqlInternalUser     = sys.env("SQL_CLIENT_USER")
    val sqlInternalPassword = sys.env("SQL_CLIENT_PASSWORD")
    val clientDb = Database.forURL(
      url =
        s"jdbc:mariadb://$sqlInternalHost:$sqlInternalPort?autoReconnect=true&useSSL=false&serverTimeZone=UTC&useUnicode=true&characterEncoding=UTF-8&usePipelineAuth=false",
      user = sqlInternalUser,
      password = sqlInternalPassword,
      driver = "org.mariadb.jdbc.Driver"
    )
    MySqlDeployConnectorImpl(clientDatabase = clientDb)(system.dispatcher)
  }

  override def functionValidator: FunctionValidator = (project: Project, fn: FunctionInput) => {
    if (fn.name == "failing") Vector(SchemaError(`type` = "model", field = "field", description = "error")) else Vector.empty
  }
}
