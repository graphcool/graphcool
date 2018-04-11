package com.prisma.deploy.connector

import com.prisma.shared.models.{Field, Model, Project}

import scala.concurrent.Future

trait DeployConnector {
  def projectPersistence: ProjectPersistence
  def migrationPersistence: MigrationPersistence
  def deployMutactionExecutor: DeployMutactionExecutor
  def clientDBQueries(project: Project): ClientDbQueries
  def databaseIntrospector: DatabaseIntrospector

  def initialize(): Future[Unit]
  def reset(): Future[Unit]
  def shutdown(): Future[Unit]

  // other methods
  def createProjectDatabase(id: String): Future[Unit]
  def deleteProjectDatabase(id: String): Future[Unit]
  def getAllDatabaseSizes(): Future[Vector[DatabaseSize]]

}

case class DatabaseSize(name: String, total: Double)

trait ClientDbQueries {
  def existsByModel(modelName: String): Future[Boolean]
  def existsByRelation(relationId: String): Future[Boolean]
  def existsNullByModelAndScalarField(model: Model, field: Field): Future[Boolean]
  def existsNullByModelAndRelationField(model: Model, field: Field): Future[Boolean]
}

trait DatabaseIntrospector {
  // `Collections` is used as a generic term to describe individual databases on a single db cluster.
  // In MySQL they are called schemas, in Mongo they are called collections

  def listCollections: Future[Vector[String]]
  def generateSchema(collection: String): Future[String]
}
