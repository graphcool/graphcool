package com.prisma.deploy.specutils

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cool.graph.cuid.Cuid
import com.prisma.shared.models.{Migration, MigrationId, Project}
import com.prisma.utils.await.AwaitUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import play.api.libs.json.{JsArray, JsString}

import scala.collection.mutable.ArrayBuffer

trait DeploySpecBase extends BeforeAndAfterEach with BeforeAndAfterAll with AwaitUtils with SprayJsonExtensions { self: Suite =>

  implicit lazy val system                                      = ActorSystem()
  implicit lazy val materializer                                = ActorMaterializer()
  implicit lazy val testDependencies: DeployDependenciesForTest = DeployDependenciesForTest()

  val server = DeployTestServer()
//  val clientDb          = testDependencies.clientTestDb
  val projectsToCleanUp = new ArrayBuffer[String]

  val basicTypesGql =
    """
      |type TestModel {
      |  id: ID! @unique
      |}
    """.stripMargin.trim()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    testDependencies.deployPersistencePlugin.initialize().await()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    testDependencies.deployPersistencePlugin.shutdown().await()
//    clientDb.shutdown()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testDependencies.deployPersistencePlugin.reset().await
//    projectsToCleanUp.foreach(clientDb.delete)
    projectsToCleanUp.clear()
  }

  def setupProject(
      schema: String,
      name: String = Cuid.createCuid(),
      stage: String = Cuid.createCuid(),
      secrets: Vector[String] = Vector.empty
  ): (Project, Migration) = {

    val projectId = name + "@" + stage
    projectsToCleanUp :+ projectId
    server.addProject(name, stage)
    server.deploySchema(name, stage, schema.stripMargin, secrets)
  }

  def formatSchema(schema: String): String = JsString(schema).toString()
  def escapeString(str: String): String    = JsString(str).toString()
}
