package com.prisma.deploy.connector.mysql

import java.sql.Driver

import com.prisma.config.DatabaseConfig
import com.prisma.connector.shared.jdbc.{Databases, SlickDatabase}
import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.MySQLProfile

case class MySqlInternalDatabaseDefs(dbConfig: DatabaseConfig, driver: Driver) {
  import slick.jdbc.MySQLProfile.api._

  lazy val setupDatabases = databases(root = true)

  lazy val managementSchemaName = dbConfig.managementSchema.getOrElse("prisma")
  lazy val managementDatabases  = databases(root = false)

  private def databases(root: Boolean): Databases = {
    val config        = typeSafeConfigFromDatabaseConfig(dbConfig, root)
    val masterDb      = Database.forConfig("database", config, driver)
    val slickDatabase = SlickDatabase(MySQLProfile, masterDb)

    Databases(primary = slickDatabase, replica = slickDatabase)
  }

  private def typeSafeConfigFromDatabaseConfig(dbConfig: DatabaseConfig, root: Boolean): Config = {
    val pooled = if (dbConfig.pooled) "" else "connectionPool = disabled"
    val schema = if (root) "" else managementSchemaName

    ConfigFactory
      .parseString(s"""
        |database {
        |  connectionInitSql="set names utf8mb4"
        |  dataSourceClass = "slick.jdbc.DriverDataSource"
        |  properties {
        |    url = "jdbc:mysql://${dbConfig.host}:${dbConfig.port}/$schema?autoReconnect=true&useSSL=${dbConfig.ssl}&requireSSL=false&serverTimeZone=UTC&useUnicode=true&characterEncoding=UTF-8&socketTimeout=60000&usePipelineAuth=false"
        |    user = "${dbConfig.user}"
        |    password = "${dbConfig.password.getOrElse("")}"
        |  }
        |  numThreads = 1
        |  connectionTimeout = 5000
        |  $pooled
        |}
      """.stripMargin)
      .resolve
  }
}
