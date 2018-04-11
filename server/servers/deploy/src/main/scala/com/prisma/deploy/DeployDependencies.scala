package com.prisma.deploy

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.prisma.auth.Auth
import com.prisma.deploy.connector.DeployConnector
import com.prisma.deploy.migration.migrator.Migrator
import com.prisma.deploy.schema.SchemaBuilder
import com.prisma.deploy.schema.mutations.FunctionValidator
import com.prisma.deploy.server.ClusterAuth
import com.prisma.errors.ErrorReporter
import com.prisma.messagebus.PubSubPublisher
import com.prisma.utils.await.AwaitUtils

import scala.concurrent.ExecutionContext

trait DeployDependencies extends AwaitUtils {
  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val reporter: ErrorReporter

  implicit def self: DeployDependencies

  def migrator: Migrator
  def clusterAuth: ClusterAuth
  def invalidationPublisher: PubSubPublisher[String]
  def apiAuth: Auth
  def deployPersistencePlugin: DeployConnector
  def functionValidator: FunctionValidator

  lazy val projectPersistence   = deployPersistencePlugin.projectPersistence
  lazy val migrationPersistence = deployPersistencePlugin.migrationPersistence
  lazy val databaseIntrospector = deployPersistencePlugin.databaseIntrospector
  lazy val clusterSchemaBuilder = SchemaBuilder()

  def initialize()(implicit ec: ExecutionContext): Unit = {
    await(deployPersistencePlugin.initialize(), seconds = 30)
    system.actorOf(Props(DatabaseSizeReporter(projectPersistence, deployPersistencePlugin)))
  }
}
