package com.prisma.integration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.prisma.api.connector.DataResolver
import com.prisma.api.connector.mysql.database.DataResolverImpl
import com.prisma.api.util.StringMatchers
import com.prisma.api.{ApiDependenciesForTest, ApiTestDatabase, ApiTestServer}
import com.prisma.deploy.specutils.{DeployDependenciesForTest, DeployTestServer}
import com.prisma.shared.models.{Migration, Project}
import com.prisma.util.json.SprayJsonExtensions
import com.prisma.utils.await.AwaitUtils
import cool.graph.cuid.Cuid
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import play.api.libs.json.JsString

import scala.collection.mutable.ArrayBuffer

trait IntegrationBaseSpec extends BeforeAndAfterEach with BeforeAndAfterAll with SprayJsonExtensions with AwaitUtils with StringMatchers { self: Suite =>

  implicit lazy val system       = ActorSystem()
  implicit lazy val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    super.afterAll()
    deployTestDependencies.deployPersistencePlugin.shutdown().await()
    apiTestDependencies.destroy
  }

  def escapeString(str: String) = JsString(str).toString()

  // API

  implicit lazy val apiTestDependencies = new ApiDependenciesForTest
  val apiServer                         = ApiTestServer()
  val apiDatabase                       = ApiTestDatabase()

  def dataResolver(project: Project): DataResolver = apiTestDependencies.dataResolver(project)

  // DEPLOY

  implicit lazy val deployTestDependencies: DeployDependenciesForTest = DeployDependenciesForTest()

  val deployServer      = DeployTestServer()
  val projectsToCleanUp = new ArrayBuffer[String]

  val basicTypesGql =
    """
      |type TestModel {
      |  id: ID! @unique
      |}
    """.stripMargin.trim()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    deployTestDependencies.deployPersistencePlugin.initialize().await()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    deployTestDependencies.deployPersistencePlugin.reset().await
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
    deployServer.addProject(name, stage)
    deployServer.deploySchema(name, stage, schema.stripMargin, secrets)
  }

  def formatSchema(schema: String): String = JsString(schema).toString()
}
