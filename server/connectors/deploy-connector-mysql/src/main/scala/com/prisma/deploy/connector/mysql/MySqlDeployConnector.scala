package com.prisma.deploy.connector.mysql

import java.sql.Driver

import com.prisma.config.DatabaseConfig
import com.prisma.deploy.connector._
import com.prisma.deploy.connector.jdbc.MySqlDatabaseInspector
import com.prisma.deploy.connector.jdbc.database.{JdbcClientDbQueries, JdbcDeployMutactionExecutor}
import com.prisma.deploy.connector.jdbc.persistence.{JdbcCloudSecretPersistence, JdbcMigrationPersistence, JdbcProjectPersistence, JdbcTelemetryPersistence}
import com.prisma.deploy.connector.mysql.database.{MySqlInternalDatabaseSchema, MySqlJdbcDeployDatabaseMutationBuilder, MySqlTypeMapper}
import com.prisma.deploy.connector.persistence.{MigrationPersistence, ProjectPersistence, TelemetryPersistence}
import com.prisma.shared.models.{ConnectorCapabilities, Project, ProjectIdEncoder}
import org.joda.time.DateTime
import slick.dbio.Effect.Read
import slick.dbio.{DBIOAction, NoStream}
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.meta.MTable

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class MySqlDeployConnector(config: DatabaseConfig, driver: Driver)(implicit ec: ExecutionContext) extends DeployConnector {

  override def capabilities: ConnectorCapabilities = ConnectorCapabilities.mysqlPrototype

  lazy val internalDatabaseDefs = MySqlInternalDatabaseDefs(config, driver)
  lazy val setupDatabases       = internalDatabaseDefs.setupDatabases
  lazy val managementDatabases  = internalDatabaseDefs.managementDatabases
  lazy val managementDatabase   = internalDatabaseDefs.managementDatabases.primary

  override lazy val projectPersistence: ProjectPersistence             = JdbcProjectPersistence(managementDatabase, config)
  override lazy val migrationPersistence: MigrationPersistence         = JdbcMigrationPersistence(managementDatabase)
  override lazy val cloudSecretPersistence: JdbcCloudSecretPersistence = JdbcCloudSecretPersistence(managementDatabase)
  override lazy val telemetryPersistence: TelemetryPersistence         = JdbcTelemetryPersistence(managementDatabase)
  override lazy val databaseInspector: DatabaseInspector               = MySqlDatabaseInspector(managementDatabase)

  lazy val mySqlTypeMapper                                      = MySqlTypeMapper()
  lazy val mutationBuilder                                      = MySqlJdbcDeployDatabaseMutationBuilder(managementDatabase, mySqlTypeMapper)
  override val deployMutactionExecutor: DeployMutactionExecutor = JdbcDeployMutactionExecutor(mutationBuilder)

  override def createProjectDatabase(id: String): Future[Unit] = {
    val action = mutationBuilder.createDatabaseForProject(id = id)
    managementDatabase.database.run(action)
  }

  override def deleteProjectDatabase(id: String): Future[Unit] = {
    val action = mutationBuilder.deleteProjectDatabase(projectId = id).map(_ => ())
    managementDatabase.database.run(action)
  }

  override def getAllDatabaseSizes(): Future[Vector[DatabaseSize]] = {
    val action = {
      val query = sql"""SELECT table_schema, sum( data_length + index_length) / 1024 / 1024 FROM information_schema.TABLES GROUP BY table_schema"""
      query.as[(String, Double)].map { tuples =>
        tuples.map { tuple =>
          DatabaseSize(tuple._1, tuple._2)
        }
      }
    }

    managementDatabase.database.run(action)
  }

  override def clientDBQueries(project: Project): ClientDbQueries      = JdbcClientDbQueries(project, managementDatabase)
  override def getOrCreateTelemetryInfo(): Future[TelemetryInfo]       = telemetryPersistence.getOrCreateInfo()
  override def updateTelemetryInfo(lastPinged: DateTime): Future[Unit] = telemetryPersistence.updateTelemetryInfo(lastPinged)
  override val projectIdEncoder: ProjectIdEncoder                      = ProjectIdEncoder('@')

  override def initialize(): Future[Unit] = {
    setupDatabases.primary.database
      .run(MySqlInternalDatabaseSchema.createSchemaActions(internalDatabaseDefs.managementSchemaName, recreate = false))
      .flatMap(_ => internalDatabaseDefs.setupDatabases.shutdown)
  }

  override def reset(): Future[Unit] = truncateTablesInDatabase(managementDatabase.database)

  override def shutdown(): Future[Unit] = managementDatabases.shutdown

  override def managementLock(): Future[Unit] = {
    managementDatabase.database.run(sql"SELECT GET_LOCK('deploy_privileges', 2);".as[Int].head.withPinnedSession).transformWith {
      case Success(result) => if (result == 1) Future.successful(()) else managementLock()
      case Failure(err)    => Future.failed(err)
    }
  }

  protected def truncateTablesInDatabase(database: Database)(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      schemas <- database.run(getTables())
      _       <- database.run(dangerouslyTruncateTables(schemas))
    } yield ()
  }

  private def getTables()(implicit ec: ExecutionContext): DBIOAction[Vector[String], NoStream, Read] = {
    for {
      metaTables <- MTable.getTables(cat = Some(internalDatabaseDefs.managementSchemaName), schemaPattern = None, namePattern = None, types = None)
    } yield metaTables.map(table => table.name.name)
  }

  private def dangerouslyTruncateTables(tableNames: Vector[String]): DBIOAction[Unit, NoStream, Effect] = {
    DBIO.seq(List(sqlu"""SET FOREIGN_KEY_CHECKS=0""") ++ tableNames.map(name => sqlu"TRUNCATE TABLE #$name") ++ List(sqlu"""SET FOREIGN_KEY_CHECKS=1"""): _*)
  }
}
