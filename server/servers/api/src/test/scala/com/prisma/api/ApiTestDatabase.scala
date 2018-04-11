package com.prisma.api

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.prisma.api.connector.mysql.MySqlApiConnectorImpl
import com.prisma.api.connector.{ApiConnector, DatabaseMutaction}
import com.prisma.api.connector.mysql.database.DatabaseQueryBuilder
import com.prisma.deploy.connector.mysql.impls.DeployMutactionExectutorImpl
import com.prisma.deploy.connector.{CreateRelationTable, DeployMutaction}
import com.prisma.shared.models._
import com.prisma.utils.await.AwaitUtils
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.MySQLProfile.backend.DatabaseDef

case class ApiTestDatabase()(implicit dependencies: ApiDependencies) extends AwaitUtils {

  implicit lazy val system: ActorSystem             = dependencies.system
  implicit lazy val materializer: ActorMaterializer = dependencies.materializer
  private lazy val clientDatabase: DatabaseDef      = dependencies.apiConnector.asInstanceOf[MySqlApiConnectorImpl].databases.master

  def setup(project: Project): Unit = {
    delete(project)
    createProjectDatabase(project)

    // The order here is very important or foreign key constraints will fail
    project.models.foreach(createModelTable(project, _))
    project.relations.foreach(createRelationTable(project, _))
  }

  def truncate(project: Project): Unit = {
    val tables = clientDatabase.run(DatabaseQueryBuilder.getTables(project.id)).await
    val dbAction = {
      val actions = List(sqlu"""USE `#${project.id}`;""") ++ List(DatabaseApiTestDatabaseMutationBuilder.dangerouslyTruncateTable(tables))
      DBIO.seq(actions: _*)
    }
    clientDatabase.run(dbAction).await()
  }

  def delete(project: Project): Unit = dropDatabases(Vector(project.id))

  private def createProjectDatabase(project: Project) = runDbActionOnClientDb(DatabaseApiTestDatabaseMutationBuilder.createClientDatabaseForProject(project.id))
  private def createModelTable(project: Project, model: Model) =
    runDbActionOnClientDb(DatabaseApiTestDatabaseMutationBuilder.createTableForModel(project.id, model))
  private def createRelationTable(project: Project, relation: Relation) = runMutaction(CreateRelationTable(project.id, project.schema, relation = relation))

  def deleteExistingDatabases(): Unit = {
    val schemas = {
      clientDatabase
        .run(DatabaseQueryBuilder.getSchemas)
        .await
        .filter(db => !Vector("information_schema", "mysql", "performance_schema", "sys", "innodb", "graphcool").contains(db))
    }
    dropDatabases(schemas)
  }

  private def dropDatabases(dbs: Vector[String]): Unit = {
    val dbAction = DBIO.seq(dbs.map(db => DatabaseApiTestDatabaseMutationBuilder.dropDatabaseIfExists(database = db)): _*)
    clientDatabase.run(dbAction).await(60)
  }

  private def runMutaction(mutaction: DeployMutaction): Unit                    = DeployMutactionExectutorImpl(clientDatabase)(system.dispatcher).execute(mutaction).await
  def runDatabaseMutactionOnClientDb(mutaction: DatabaseMutaction)              = dependencies.databaseMutactionExecutor.execute(Vector(mutaction)).await()
  def runDbActionOnClientDb(action: DBIOAction[Any, NoStream, Effect.All]): Any = clientDatabase.run(action).await()
}
