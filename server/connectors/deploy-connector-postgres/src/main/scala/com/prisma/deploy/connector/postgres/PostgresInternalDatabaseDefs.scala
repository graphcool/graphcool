package com.prisma.deploy.connector.postgres

import java.sql.Driver

import com.prisma.config.DatabaseConfig
import com.prisma.connector.shared.jdbc.{Databases, SlickDatabase}
import slick.jdbc.{DataSourceJdbcDataSource, DriverDataSource, PostgresProfile}

case class PostgresInternalDatabaseDefs(dbConfig: DatabaseConfig, driver: Driver) {
  import slick.jdbc.PostgresProfile.api._

  // Only used during setup - this is the default PSQL db, which is only used for administrative commands
  lazy val setupDatabases = getDatabase("postgres", "public")

  // Used during runtime & setup
  lazy val dbName               = dbConfig.database.getOrElse("prisma")
  lazy val managementSchemaName = dbConfig.managementSchema.getOrElse("management")
  lazy val managementDatabases  = getDatabase(dbName, managementSchemaName)

  private def getDatabase(dbToUse: String, schemaToUse: String): Databases = {
    val masterDb      = databaseForConfig(dbToUse, schemaToUse)
    val slickDatabase = SlickDatabase(PostgresProfile, masterDb)

    Databases(primary = slickDatabase, replica = slickDatabase)
  }

  private def databaseForConfig(database: String, schema: String) = {
    val source         = simpleDataSource(database, schema)
    val poolName       = "database"
    val numThreads     = 1
    val maxConnections = 1
    val executor       = AsyncExecutor(poolName, numThreads, numThreads, 1000, maxConnections, registerMbeans = false)
    Database.forSource(source, executor)
  }

  private def simpleDataSource(database: String, schema: String) = {
    new DataSourceJdbcDataSource(
      new DriverDataSource(
        url =
          s"jdbc:postgresql://${dbConfig.host}:${dbConfig.port}/$database?currentSchema=$schema&ssl=${dbConfig.ssl}&sslfactory=org.postgresql.ssl.NonValidatingFactory",
        user = dbConfig.user,
        password = dbConfig.password.getOrElse(""),
        driverObject = driver
      ),
      keepAliveConnection = true,
      maxConnections = None
    )
  }
}
