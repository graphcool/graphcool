package com.prisma.api.connector.mysql

import java.sql.Driver

import com.prisma.api.connector.ApiConnector
import com.prisma.api.connector.jdbc.impl.{JdbcDataResolver, JdbcDatabaseMutactionExecutor}
import com.prisma.config.DatabaseConfig
import com.prisma.shared.models.{ConnectorCapabilities, Project, ProjectIdEncoder}

import scala.concurrent.{ExecutionContext, Future}

case class MySqlApiConnector(config: DatabaseConfig, driver: Driver)(implicit ec: ExecutionContext) extends ApiConnector {

  override val capabilities = ConnectorCapabilities.mysqlPrototype

  lazy val databases = MySqlDatabasesFactory.initialize(config, driver)

  override def initialize() = {
    databases
    Future.unit
  }

  override def shutdown() = {
    for {
      _ <- databases.primary.database.shutdown
      _ <- databases.replica.database.shutdown
    } yield ()
  }

  override val databaseMutactionExecutor            = JdbcDatabaseMutactionExecutor(databases.primary)
  override val projectIdEncoder: ProjectIdEncoder   = ProjectIdEncoder('@')
  override def dataResolver(project: Project)       = JdbcDataResolver(project, databases.replica)(ec)
  override def masterDataResolver(project: Project) = JdbcDataResolver(project, databases.primary)(ec)
}
