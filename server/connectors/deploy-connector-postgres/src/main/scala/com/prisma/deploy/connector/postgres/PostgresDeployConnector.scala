package com.prisma.deploy.connector.postgres

import java.sql.Driver

import com.prisma.config.DatabaseConfig
import com.prisma.deploy.connector._
import com.prisma.deploy.connector.jdbc.PostgresDatabaseInspector
import com.prisma.deploy.connector.jdbc.database.{JdbcClientDbQueries, JdbcDeployMutactionExecutor}
import com.prisma.deploy.connector.jdbc.persistence.{JdbcCloudSecretPersistence, JdbcMigrationPersistence, JdbcProjectPersistence, JdbcTelemetryPersistence}
import com.prisma.deploy.connector.persistence.{CloudSecretPersistence, MigrationPersistence, ProjectPersistence, TelemetryPersistence}
import com.prisma.deploy.connector.postgres.database._
import com.prisma.shared.models.{ConnectorCapabilities, Project, ProjectIdEncoder}
import org.joda.time.DateTime
import slick.dbio.Effect.Read
import slick.dbio.{DBIOAction, NoStream}
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class PostgresDeployConnector(dbConfig: DatabaseConfig, driver: Driver)(implicit ec: ExecutionContext) extends DeployConnector {

  override def capabilities: ConnectorCapabilities = ConnectorCapabilities.postgresPrototype

  lazy val internalDatabaseDefs = PostgresInternalDatabaseDefs(dbConfig, driver)
  lazy val setupDatabases       = internalDatabaseDefs.setupDatabases
  lazy val managementDatabases  = internalDatabaseDefs.managementDatabases
  lazy val managementDatabase   = internalDatabaseDefs.managementDatabases.primary

  override lazy val projectPersistence: ProjectPersistence         = JdbcProjectPersistence(managementDatabase, dbConfig)
  override lazy val migrationPersistence: MigrationPersistence     = JdbcMigrationPersistence(managementDatabase)
  override lazy val cloudSecretPersistence: CloudSecretPersistence = JdbcCloudSecretPersistence(managementDatabase)
  override lazy val telemetryPersistence: TelemetryPersistence     = JdbcTelemetryPersistence(managementDatabase)
  override lazy val databaseInspector: DatabaseInspector           = PostgresDatabaseInspector(managementDatabase)

  lazy val postgresTypeMapper                                        = PostgresTypeMapper()
  lazy val mutationBuilder                                           = PostgresJdbcDeployDatabaseMutationBuilder(managementDatabase, postgresTypeMapper)
  override lazy val deployMutactionExecutor: DeployMutactionExecutor = JdbcDeployMutactionExecutor(mutationBuilder)

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
      val query = sql"""
           SELECT schemaname, SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) / 1024 / 1024 FROM pg_tables GROUP BY schemaname
         """
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
  override val projectIdEncoder: ProjectIdEncoder                      = ProjectIdEncoder('$')

  override def initialize(): Future[Unit] = {
    // We're ignoring failures for createDatabaseAction as there is no "create if not exists" in psql
    setupDatabases.primary.database
      .run(InternalDatabaseSchema.createDatabaseAction(internalDatabaseDefs.dbName))
      .transformWith { _ =>
        val action = InternalDatabaseSchema.createSchemaActions(internalDatabaseDefs.managementSchemaName, recreate = false)
        managementDatabase.database.run(action)
      }
      .flatMap(_ => setupDatabases.shutdown)
  }

  override def reset(): Future[Unit] = truncateManagementTablesInDatabase(managementDatabase.database)

  override def shutdown(): Future[Unit] = managementDatabases.shutdown

  override def managementLock(): Future[Unit] = {
    managementDatabase.database.run(sql"SELECT pg_advisory_lock(1000);".as[String].head.withPinnedSession).transformWith {
      case Success(_)   => Future.successful(())
      case Failure(err) => Future.failed(err)
    }
  }

  protected def truncateManagementTablesInDatabase(database: Database)(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      schemas <- database.run(getTables())
      _       <- database.run(dangerouslyTruncateTables(schemas))
    } yield ()
  }

  private def getTables()(implicit ec: ExecutionContext): DBIOAction[Vector[String], NoStream, Read] = {
    sql"""SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = '#${internalDatabaseDefs.managementSchemaName}'
          AND table_type = 'BASE TABLE';""".as[String]
  }

  private def dangerouslyTruncateTables(tableNames: Vector[String]): DBIOAction[Unit, NoStream, Effect] = {
    DBIO.seq(tableNames.map(name => sqlu"""TRUNCATE TABLE "#$name" cascade"""): _*)
  }
}
