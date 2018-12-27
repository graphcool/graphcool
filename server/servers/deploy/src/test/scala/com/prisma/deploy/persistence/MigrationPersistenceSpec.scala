package com.prisma.deploy.persistence

import com.prisma.deploy.specutils.{DeploySpecBase, TestProject}
import com.prisma.shared.models._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class MigrationPersistenceSpec extends FlatSpec with Matchers with DeploySpecBase {

  val migrationPersistence = testDependencies.migrationPersistence
  val projectPersistence   = testDependencies.projectPersistence

  ".byId()" should "load a migration by project ID and revision" in {
    val (project1, _) = setupProject(basicTypesGql, stage = "stage1")
    val (project2, _) = setupProject(basicTypesGql, stage = "stage2")

    val migration0Project1 = migrationPersistence.byId(MigrationId(project1.id, 1)).await.get
    migration0Project1.projectId shouldEqual project1.id
    migration0Project1.revision shouldEqual 1

    val migration0Project2 = migrationPersistence.byId(MigrationId(project2.id, 1)).await.get
    migration0Project2.projectId shouldEqual project2.id
    migration0Project2.revision shouldEqual 1
  }

  ".byId()" should "load a migration by project ID and revision with the previous schema" in {
    val (project1, initialMigration) = setupProject(basicTypesGql)
    val newMigration                 = migrationPersistence.create(migrationWithRandomSchema(project1.id)).await()

    val loadedMigration = migrationPersistence.byId(MigrationId(newMigration.projectId, newMigration.revision)).await.get
    loadedMigration.previousSchema should equal(initialMigration.schema)
  }

  ".byId()" should "put always the schema of the last successful migration as a previous schema" in {
    val (project, initialMigration) = setupProject(basicTypesGql)
    initialMigration.revision should be(2)
    migrationPersistence.create(migrationWithRandomSchema(project.id)).await
    migrationPersistence.create(migrationWithRandomSchema(project.id)).await
    migrationPersistence.create(migrationWithRandomSchema(project.id).copy(status = MigrationStatus.Success)).await
    migrationPersistence.create(migrationWithRandomSchema(project.id)).await

    val migration3 = migrationPersistence.byId(MigrationId(project.id, 3)).await.get
    val migration4 = migrationPersistence.byId(MigrationId(project.id, 4)).await.get
    val migration5 = migrationPersistence.byId(MigrationId(project.id, 5)).await.get
    val migration6 = migrationPersistence.byId(MigrationId(project.id, 6)).await.get

    migration3.previousSchema should be(initialMigration.schema)
    migration4.previousSchema should be(initialMigration.schema)
    migration5.previousSchema should be(initialMigration.schema)
    migration6.previousSchema should be(migration5.schema)
  }

  ".create()" should "store the migration in the db, increment the revision accordingly and return the previous schema" in {
    val (project, _) = setupProject(basicTypesGql)
    migrationPersistence.loadAll(project.id).await should have(size(2))

    val rawDataModel      = "type User { id: ID }"
    val savedMigration    = migrationPersistence.create(migrationWithRandomSchema(project.id, rawDataModel = rawDataModel)).await()
    val previousMigration = migrationPersistence.loadAll(project.id).await.apply(1)
    savedMigration.revision shouldEqual 3
    savedMigration.previousSchema should equal(previousMigration.schema)
    savedMigration.rawDataModel should be(rawDataModel)
    migrationPersistence.loadAll(project.id).await should have(size(3))
  }

  ".create()" should "store the migration with its function in the db" in {
    val (project, initialMigration) = setupProject(basicTypesGql)
    val function = ServerSideSubscriptionFunction(
      name = "my-function",
      isActive = true,
      delivery = WebhookDelivery("https://mywebhook.com", Vector("header1" -> "value1")),
      query = "query"
    )
    val rawDataModel     = "type User { id: ID }"
    val migration        = Migration.empty(project.id).copy(functions = Vector(function), status = MigrationStatus.Success, rawDataModel = rawDataModel)
    val createdMigration = migrationPersistence.create(migration).await()
    createdMigration.previousSchema should equal(initialMigration.schema)
    createdMigration.rawDataModel should be(rawDataModel)

    val inDb = migrationPersistence.getLastMigration(project.id).await().get
    inDb.functions should equal(Vector(function))
    inDb.rawDataModel should be(rawDataModel)
  }

  ".loadAll()" should "return all migrations for a project" in {
    val (project, _) = setupProject(basicTypesGql)

    // 2 successful, 2 pending migrations (+ 1 from setup)
    migrationPersistence.create(migrationWithRandomSchema(project.id).copy(status = MigrationStatus.Success)).await
    migrationPersistence.create(migrationWithRandomSchema(project.id)).await
    migrationPersistence.create(migrationWithRandomSchema(project.id)).await
    migrationPersistence.create(migrationWithRandomSchema(project.id).copy(status = MigrationStatus.Success)).await

    val migrations = migrationPersistence.loadAll(project.id).await
    migrations should have(size(6))
    val successfulMigration1 = migrations.find(_.revision == 3).get
    val pendingMigration1    = migrations.find(_.revision == 4).get
    val pendingMigration2    = migrations.find(_.revision == 5).get
    val successfulMigration2 = migrations.find(_.revision == 6).get
    pendingMigration1.previousSchema should equal(successfulMigration1.schema)
    pendingMigration2.previousSchema should equal(successfulMigration1.schema)
    successfulMigration2.previousSchema should equal(successfulMigration1.schema)
  }

  ".updateMigrationStatus()" should "update a migration status correctly" in {
    val (project, _)     = setupProject(basicTypesGql)
    val createdMigration = migrationPersistence.create(Migration.empty(project.id)).await

    migrationPersistence.updateMigrationStatus(createdMigration.id, MigrationStatus.Success).await

    val lastMigration = migrationPersistence.getLastMigration(project.id).await.get
    lastMigration.revision shouldEqual createdMigration.revision
    lastMigration.status shouldEqual MigrationStatus.Success
  }

  ".updateMigrationErrors()" should "update the migration errors correctly" in {
    val (project, _)     = setupProject(basicTypesGql)
    val createdMigration = migrationPersistence.create(Migration.empty(project.id)).await
    val errors           = Vector("This is a serious issue", "Another one, oh noes.")

    migrationPersistence.updateMigrationErrors(createdMigration.id, errors).await

    val reloadedMigration = migrationPersistence.byId(createdMigration.id).await.get
    reloadedMigration.errors shouldEqual errors
  }

  ".updateMigrationApplied()" should "update the migration applied counter correctly" in {
    val (project, _)     = setupProject(basicTypesGql)
    val createdMigration = migrationPersistence.create(Migration.empty(project.id)).await

    migrationPersistence.updateMigrationApplied(createdMigration.id, 1).await

    val reloadedMigration = migrationPersistence.byId(createdMigration.id).await.get
    reloadedMigration.applied shouldEqual 1
  }

  ".updateMigrationRolledBack()" should "update the migration rolled back counter correctly" in {
    val (project, _)     = setupProject(basicTypesGql)
    val createdMigration = migrationPersistence.create(Migration.empty(project.id)).await

    migrationPersistence.updateMigrationRolledBack(createdMigration.id, 1).await

    val reloadedMigration = migrationPersistence.byId(createdMigration.id).await.get
    reloadedMigration.rolledBack shouldEqual 1
  }

  ".updateMigrationStartedAt()" should "update the migration startedAt timestamp correctly" in {
    val (project, _)     = setupProject(basicTypesGql)
    val createdMigration = migrationPersistence.create(Migration.empty(project.id)).await
    val time             = DateTime.now()

    migrationPersistence.updateStartedAt(createdMigration.id, time).await

    val reloadedMigration = migrationPersistence.byId(createdMigration.id).await.get
    reloadedMigration.startedAt.isDefined shouldEqual true // some bug causes mysql timstamps to be off by a margin, equal is broken
  }

  ".updateMigrationFinishedAt()" should "update the migration finishedAt timestamp correctly" in {
    val (project, _)     = setupProject(basicTypesGql)
    val createdMigration = migrationPersistence.create(Migration.empty(project.id)).await
    val time             = DateTime.now()

    migrationPersistence.updateFinishedAt(createdMigration.id, time).await

    val reloadedMigration = migrationPersistence.byId(createdMigration.id).await.get
    reloadedMigration.finishedAt.isDefined shouldEqual true // some bug causes mysql timstamps to be off by a margin, equal is broken
  }

  ".getLastMigration()" should "get the last migration applied to a project" in {
    val (project, initialSchema) = setupProject(basicTypesGql)
    val createdMigration         = migrationPersistence.create(Migration.empty(project.id)).await
    migrationPersistence.updateMigrationStatus(createdMigration.id, MigrationStatus.Success).await
    val lastMigration = migrationPersistence.getLastMigration(project.id).await.get
    lastMigration.revision should equal(3)
    lastMigration.previousSchema should equal(initialSchema.schema)
  }

  ".getNextMigration()" should "get the next migration to be applied to a project" in {
    val (project, initialMigration) = setupProject(basicTypesGql)
    val createdMigration            = migrationPersistence.create(Migration.empty(project.id)).await

    val nextMigration = migrationPersistence.getNextMigration(project.id).await.get
    nextMigration.revision shouldEqual createdMigration.revision
    nextMigration.previousSchema should equal(initialMigration.schema)
  }

  "loadDistinctUnmigratedProjectIds()" should "load all distinct project ids that have open migrations" in {
    val migratedProject               = TestProject()
    val unmigratedProject             = TestProject()
    val unmigratedProjectWithMultiple = TestProject()

    // Create base projects
    projectPersistence.create(migratedProject).await()
    projectPersistence.create(unmigratedProject).await()
    projectPersistence.create(unmigratedProjectWithMultiple).await()

    // Create pending migrations
    migrationPersistence.create(Migration.empty(unmigratedProject.id)).await
    migrationPersistence.create(Migration.empty(unmigratedProjectWithMultiple.id)).await
    migrationPersistence.create(Migration.empty(unmigratedProjectWithMultiple.id)).await

    val projectIds = migrationPersistence.loadDistinctUnmigratedProjectIds().await
    projectIds should have(size(2))
  }

  def migrationWithRandomSchema(projectId: String, rawDataModel: String = ""): Migration = {
    Migration(
      projectId = projectId,
      schema = randomSchema(),
      steps = Vector.empty,
      functions = Vector.empty,
      rawDataModel = rawDataModel
    )
  }

  def randomSchema(): Schema = {
    def randomName = cool.graph.cuid.Cuid.createCuid()
    val model = ModelTemplate(
      name = randomName,
      stableIdentifier = "don't care",
      isEmbedded = false,
      fieldTemplates = List.empty,
      manifestation = None,
      indexes = Vector(),
    )

    Schema(
      modelTemplates = List(model),
      relationTemplates = List.empty,
      enums = List.empty,
      version = None
    )
  }

}
