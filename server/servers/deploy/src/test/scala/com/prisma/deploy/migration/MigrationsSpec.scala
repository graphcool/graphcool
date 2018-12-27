package com.prisma.deploy.migration

import com.prisma.deploy.connector.{EmptyDatabaseIntrospectionInferrer, FieldRequirementsInterface, ForeignKey, Tables}
import com.prisma.deploy.migration.inference.{MigrationStepsInferrer, SchemaInferrer}
import com.prisma.deploy.schema.mutations.{DeployMutation, DeployMutationInput, MutationError, MutationSuccess}
import com.prisma.deploy.specutils.DeploySpecBase
import com.prisma.shared.models.ConnectorCapability._
import com.prisma.shared.models.{ConnectorCapabilities, Project, Schema}
import org.scalatest.{Matchers, WordSpecLike}

class MigrationsSpec extends WordSpecLike with Matchers with DeploySpecBase {

  override def runOnlyForCapabilities = Set(MigrationsCapability)

  val name      = this.getClass.getSimpleName
  val stage     = "default"
  val serviceId = testDependencies.projectIdEncoder.toEncodedString(name, stage)
  val initialDataModel =
    """
      |type A {
      |  id: ID! @id
      |}
    """.stripMargin
  val inspector        = deployConnector.testFacilities.inspector
  var project: Project = Project(id = serviceId, schema = Schema.empty)

  import system.dispatcher

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    setup()
  }

  val TI = com.prisma.shared.models.TypeIdentifier

  "adding a scalar field should work" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  string: String
        |  float: Float
        |  boolean: Boolean
        |  enum: MyEnum
        |  json: Json
        |  dateTime: DateTime
        |  cuid: ID
        |  int: Int
        |}
        |enum MyEnum {
        |  A,
        |  B
        |}
      """.stripMargin

    val result = deploy(dataModel)
    val table  = result.table_!("A")

    table.columns.filter(_.name != "id").foreach { column =>
      column.isRequired should be(false)
    }

    table.column_!("string").typeIdentifier should be(TI.String)
    table.column_!("float").typeIdentifier should be(TI.Float)
    table.column_!("boolean").typeIdentifier should be(TI.Boolean)
    table.column_!("enum").typeIdentifier should be(TI.String)
    table.column_!("json").typeIdentifier should be(TI.String)
    table.column_!("dateTime").typeIdentifier should be(TI.DateTime)
    table.column_!("cuid").typeIdentifier should be(TI.String)
    table.column_!("int").typeIdentifier should be(TI.Int)

//    column.tpe should be("text") // TODO: make specific type assertions vendor specific
  }

  "adding a required scalar field should work" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String!
        |}
      """.stripMargin

    val result = deploy(dataModel)

    val column = result.table_!("A").column_!("field")
    column.isRequired should be(true)
  }

  "removing a scalar field should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column("field").isDefined should be(true)

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |}
      """.stripMargin
    val result = deploy(newDataModel)
    result.table_!("A").column("field").isDefined should be(false)
  }

  "updating the type of a scalar field should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column_!("field").typeIdentifier should be(TI.String)

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: Int
        |}
      """.stripMargin
    val result = deploy(newDataModel)
    result.table_!("A").column_!("field").typeIdentifier should be(TI.Int)
  }

  "changing the type of an id field should work" in {
    val capabilities = ConnectorCapabilities(IntIdCapability)
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |}
        |type B {
        |  id: ID! @id
        |  a: A @relation(link: INLINE)
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel, capabilities)
    initialResult.table_!("A").column_!("id").typeIdentifier should be(TI.String)
    initialResult.table_!("B").column_!("a").typeIdentifier should be(TI.String)
    initialResult.table_!("B").column_!("a").foreignKey should be(Some(ForeignKey("A", "id")))

    val newDataModel =
      """
        |type A {
        |  id: Int! @id
        |}
        |type B {
        |  id: ID! @id
        |  a: A @relation(link: INLINE)
        |}
      """.stripMargin
    val result = deploy(newDataModel, capabilities)
    result.table_!("A").column_!("id").typeIdentifier should be(TI.Int)
    result.table_!("B").column_!("a").typeIdentifier should be(TI.Int)
    result.table_!("B").column_!("a").foreignKey should be(Some(ForeignKey("A", "id")))
  }

  "updating db name of a scalar field should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String @db(name: "name1")
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column("name1").isDefined should be(true)

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String @db(name: "name2")
        |}
      """.stripMargin
    val result = deploy(newDataModel)
    result.table_!("A").column("name1").isDefined should be(false)
    result.table_!("A").column("name2").isDefined should be(true)
  }

  "changing a relation field to a scalar field should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column_!("b").foreignKey should be(Some(ForeignKey("B", "id")))

    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: String
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin
    val result = deploy(dataModel)
    result.table_!("A").column_!("b").typeIdentifier should be(TI.String)
  }

  "changing a scalar field to a relation field should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: String
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin
    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column_!("b").typeIdentifier should be(TI.String)

    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val result = deploy(dataModel)
    result.table_!("A").column_!("b").foreignKey should be(Some(ForeignKey("B", "id")))
  }

  "adding a plain many to many relation should result in our plain relation table" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  bs: [B]
        |}
        |
        |type B {
        |  id: ID! @id
        |  as: [A]
        |}
      """.stripMargin

    val result        = deploy(dataModel)
    val relationTable = result.table_!("AToB")
    relationTable.columns should have(size(3))
    relationTable.column_!("id").typeIdentifier should be(TI.String)
    val aColumn = relationTable.column_!("A")
    aColumn.typeIdentifier should be(TI.String)
    aColumn.foreignKey should be(Some(ForeignKey("A", "id")))
    val bColumn = relationTable.column_!("B")
    bColumn.typeIdentifier should be(TI.String)
    bColumn.foreignKey should be(Some(ForeignKey("B", "id")))
  }

  "adding a plain many to many relation for exotic id types must also work" in {
    val dataModel =
      """
        |type A {
        |  id: Int! @id
        |  bs: [B]
        |}
        |
        |type B {
        |  id: UUID! @id
        |  as: [A]
        |}
      """.stripMargin

    val result        = deploy(dataModel, ConnectorCapabilities(IntIdCapability, UuidIdCapability))
    val relationTable = result.table_!("AToB")
    relationTable.columns should have(size(3))
    relationTable.column_!("id").typeIdentifier should be(TI.String)
    val aColumn = relationTable.column_!("A")
    aColumn.typeIdentifier should be(TI.Int)
    aColumn.foreignKey should be(Some(ForeignKey("A", "id")))
    val bColumn = relationTable.column_!("B")
    if (deployConnector.capabilities.has(UuidIdCapability)) {
      bColumn.typeIdentifier should be(TI.UUID)
    } else {
      // on MySQL UUID maps to String
      bColumn.typeIdentifier should be(TI.String)
    }
    bColumn.foreignKey should be(Some(ForeignKey("B", "id")))
  }

  "forcing a relation table for a one to many relation must be possible" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  bs: [B] @relation(link: TABLE)
        |}
        |
        |type B {
        |  id: Int! @id
        |  a: A
        |}
      """.stripMargin

    val result        = deploy(dataModel, ConnectorCapabilities(RelationLinkTableCapability, IntIdCapability))
    val relationTable = result.table_!("AToB")
    relationTable.columns should have(size(3))
    relationTable.column_!("id").typeIdentifier should be(TI.String)
    val aColumn = relationTable.column_!("A")
    aColumn.typeIdentifier should be(TI.String)
    aColumn.foreignKey should be(Some(ForeignKey("A", "id")))
    val bColumn = relationTable.column_!("B")
    bColumn.typeIdentifier should be(TI.Int)
    bColumn.foreignKey should be(Some(ForeignKey("B", "id")))
  }

  "providing an explicit link table must work" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  bs: [B] @relation(name: "CustomLinkTable", link: TABLE)
        |}
        |
        |type B {
        |  id: Int! @id
        |  a: A @relation(name: "CustomLinkTable")
        |}
        |
        |
        |type CustomLinkTable @linkTable {
        |  # those fields are intentionally in reverse lexicographical order to test they are correctly detected
        |  myB: B
        |  myA: A
        |}
      """.stripMargin

    val result        = deploy(dataModel, ConnectorCapabilities(RelationLinkTableCapability, IntIdCapability))
    val relationTable = result.table_!("CustomLinkTable")
    relationTable.columns should have(size(3))
    relationTable.column_!("id").typeIdentifier should be(TI.String)
    val aColumn = relationTable.column_!("myA")
    aColumn.typeIdentifier should be(TI.String)
    aColumn.foreignKey should be(Some(ForeignKey("A", "id")))
    val bColumn = relationTable.column_!("myB")
    bColumn.typeIdentifier should be(TI.Int)
    bColumn.foreignKey should be(Some(ForeignKey("B", "id")))
  }

  "switching models in a link table must work" in {
    val capas = ConnectorCapabilities(RelationLinkTableCapability, IntIdCapability)
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  bs: [B] @relation(name: "CustomLinkTable", link: TABLE)
        |}
        |
        |type B {
        |  id: Int! @id
        |  a: A @relation(name: "CustomLinkTable")
        |}
        |
        |type C {
        |  id: ID! @id
        |}
        |
        |type CustomLinkTable @linkTable {
        |  one: A
        |  two: B
        |}
      """.stripMargin
    val initialResult = deploy(initialDataModel, capas)

    {
      val relationTable = initialResult.table_!("CustomLinkTable")
      val oneColumn     = relationTable.column_!("one")
      oneColumn.typeIdentifier should be(TI.String)
      oneColumn.foreignKey should be(Some(ForeignKey("A", "id")))
      val twoColumn = relationTable.column_!("two")
      twoColumn.typeIdentifier should be(TI.Int)
      twoColumn.foreignKey should be(Some(ForeignKey("B", "id")))
    }

    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  cs: [C] @relation(name: "CustomLinkTable", link: TABLE)
        |}
        |
        |type B {
        |  id: Int! @id
        |}
        |
        |type C {
        |  id: ID! @id
        |}
        |
        |type CustomLinkTable @linkTable {
        |  one: A
        |  two: C
        |}
      """.stripMargin

    {
      val result        = deploy(dataModel, capas)
      val relationTable = result.table_!("CustomLinkTable")
      val oneColumn     = relationTable.column_!("one")
      oneColumn.typeIdentifier should be(TI.String)
      oneColumn.foreignKey should be(Some(ForeignKey("A", "id")))
      val twoColumn = relationTable.column_!("two")
      twoColumn.typeIdentifier should be(TI.String)
      twoColumn.foreignKey should be(Some(ForeignKey("C", "id")))
    }
  }

  "removing an explicit link table must work" in {
    val capas = ConnectorCapabilities(RelationLinkTableCapability)
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  bs: [B] @relation(name: "CustomLinkTable", link: TABLE)
        |}
        |
        |type B {
        |  id: ID! @id
        |  a: A @relation(name: "CustomLinkTable")
        |}
        |
        |type CustomLinkTable @linkTable {
        |  myA: A
        |  myB: B
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel, capas)
    initialResult.table("CustomLinkTable").isDefined should be(true)

    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  bs: [B] @relation(link: TABLE)
        |}
        |
        |type B {
        |  id: ID! @id
        |  a: A
        |}
      """.stripMargin
    val result = deploy(dataModel, capas)
    result.table("CustomLinkTable").isDefined should be(false)
    val relationTable = result.table_!("AToB")
    relationTable.columns should have(size(3))
    relationTable.column_!("id").typeIdentifier should be(TI.String)
    val aColumn = relationTable.column_!("A")
    aColumn.typeIdentifier should be(TI.String)
    aColumn.foreignKey should be(Some(ForeignKey("A", "id")))
    val bColumn = relationTable.column_!("B")
    bColumn.typeIdentifier should be(TI.String)
    bColumn.foreignKey should be(Some(ForeignKey("B", "id")))
  }

  "adding an inline relation should result in a foreign key in the model table" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val result = deploy(dataModel)

    val bColumn = result.table_!("A").column_!("b")
    bColumn.foreignKey should equal(Some(ForeignKey("B", "id")))
    bColumn.typeIdentifier should be(TI.String)
  }

  "specifying the db name of an inline relation field must work" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE) @db(name: "b_column")
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val result  = deploy(dataModel)
    val bColumn = result.table_!("A").column_!("b_column")
    bColumn.foreignKey should equal(Some(ForeignKey("B", "id")))
  }

  "adding an inline relation to an model that has as id field of an exotic type should work" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |  c: C @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: Int! @id
        |}
        |
        |type C {
        |  id: UUID! @id
        |}
      """.stripMargin

    val result = deploy(dataModel, ConnectorCapabilities(IntIdCapability, UuidIdCapability))

    val bColumn = result.table_!("A").column_!("b")
    bColumn.foreignKey should equal(Some(ForeignKey("B", "id")))
    bColumn.typeIdentifier should be(TI.Int)

    val cColumn = result.table_!("A").column_!("c")
    cColumn.foreignKey should equal(Some(ForeignKey("C", "id")))
    if (deployConnector.capabilities.has(UuidIdCapability)) {
      cColumn.typeIdentifier should be(TI.UUID)
    } else {
      // on MySQL this maps to a String
      cColumn.typeIdentifier should be(TI.String)
    }
  }

  "removing an inline relation link should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column("b").isDefined should be(true)

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin
    val result = deploy(newDataModel)
    result.table_!("A").column("b").isDefined should be(false)
  }

  "moving an inline relation link to the other side should work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel)
    initialResult.table_!("A").column("b").isDefined should be(true)

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |}
        |
        |type B {
        |  id: ID! @id
        |  a: A @relation(link: INLINE)
        |}
      """.stripMargin
    val result = deploy(newDataModel)
    result.table_!("A").column("b").isDefined should be(false)
    val aColumn = result.table_!("B").column_!("a")
    aColumn.foreignKey should be(Some(ForeignKey("A", "id")))
  }

  "converting an inline relation to a link table should work" in {
    val capas = ConnectorCapabilities(RelationLinkTableCapability)
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    val initialResult = deploy(initialDataModel, capas)
    initialResult.table_!("A").column("b").isDefined should be(true)

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: TABLE)
        |}
        |
        |type B {
        |  id: ID! @id
        |  a: A
        |}
      """.stripMargin
    val result = deploy(newDataModel, capas)
    result.table_!("A").column("b").isDefined should be(false)
    result.table("AToB").isDefined should be(true)
  }

  "converting a link table to an inline relation should work" in {
    val capas = ConnectorCapabilities(RelationLinkTableCapability)
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: TABLE)
        |}
        |
        |type B {
        |  id: ID! @id
        |  a: A
        |}
      """.stripMargin

    {
      val result = deploy(initialDataModel, capas)
      result.table_!("A").column("b").isDefined should be(false)
      result.table("AToB").isDefined should be(true)
    }

    val newDataModel =
      """
        |type A {
        |  id: ID! @id
        |  b: B @relation(link: INLINE)
        |}
        |
        |type B {
        |  id: ID! @id
        |}
      """.stripMargin

    {
      val initialResult = deploy(newDataModel, capas)
      initialResult.table_!("A").column("b").isDefined should be(true)
    }
  }

  "adding an inline self relation should add the relation link in the right column" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  a1: A @relation(name: "Selfie")
        |  a2: A @relation(name: "Selfie", link: INLINE)
        |  b1: A @relation(name: "Selfie2", link: INLINE)
        |  b2: A @relation(name: "Selfie2")
        |}
      """.stripMargin
    // testing with 2 self relations to make sure choosing the column for the foreign key is not due to lexicographic order
    val result = deploy(dataModel)
    result.table_!("A").column_!("a2").foreignKey should equal(Some(ForeignKey("A", "id")))
    result.table_!("A").column_!("b1").foreignKey should equal(Some(ForeignKey("A", "id")))
  }

  "adding a unique constraint must work" in {
    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String @unique
        |}
      """.stripMargin
    val result = deploy(dataModel)
    val index  = result.table_!("A").indexes.find(_.columns == Vector("field")).get
    index.columns should be(Vector("field"))
    index.unique should be(true)
  }

  "removing a unique constraint must work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String @unique
        |}
      """.stripMargin
    deploy(initialDataModel)

    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String
        |}
      """.stripMargin
    val result = deploy(dataModel)
    val index  = result.table_!("A").indexes.find(_.columns == Vector("field"))
    index should be(empty)
  }

  "adding an index must work" in {
    val initialDataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String
        |  shield: String
        |}
      """.stripMargin
    deploy(initialDataModel)

    val dataModel =
      """
        |type A @indexes(value: [
        |  { fields: ["field" "shield"] name: "A_field_shield_idx" }
        |]) {
        |  id: ID! @id
        |  field: String
        |  shield: String
        |}
      """.stripMargin
    val result = deploy(dataModel, ConnectorCapabilities(IndexesCapability))
    val index  = result.table_!("A").indexes.find(_.name == "A_field_shield_idx").get

    index.columns.size should be(2)
    index.columns should contain("field")
    index.columns should contain("shield")
  }

  "deleting an index must work" in {
    val initialDataModel =
      """
        |type A @indexes(value: [
        |  { fields: ["field" "shield"] name: "A_field_shield_idx" }
        |]) {
        |  id: ID! @id
        |  field: String
        |  shield: String
        |}
      """.stripMargin
    deploy(initialDataModel, ConnectorCapabilities(IndexesCapability))

    val dataModel =
      """
        |type A {
        |  id: ID! @id
        |  field: String
        |  shield: String
        |}
      """.stripMargin
    val result = deploy(dataModel, ConnectorCapabilities(IndexesCapability))
    val index  = result.table_!("A").indexes.find(_.name == "A_field_shield_idx")

    index should be(None)
  }

  "adding a field to an index must work" in {
    val initialDataModel =
      """
        |type A @indexes(value: [
        |  { fields: ["field"] name: "A_field_idx" }
        |]) {
        |  id: ID! @id
        |  field: String
        |}
      """.stripMargin
    var result = deploy(initialDataModel, ConnectorCapabilities(IndexesCapability))
    var index  = result.table_!("A").indexes.find(_.name == "A_field_idx").get

    index.columns.size should be(1)
    index.columns should contain("field")

    val dataModel =
      """
        |type A @indexes(value: [
        |  { fields: ["field" "shield"] name: "A_field_idx" }
        |]) {
        |  id: ID! @id
        |  field: String
        |  shield: String
        |}
      """.stripMargin
    result = deploy(dataModel, ConnectorCapabilities(IndexesCapability))
    index  = result.table_!("A").indexes.find(_.name == "A_field_idx").get

    index.columns.size should be(2)
    index.columns should contain("field")
    index.columns should contain("shield")
  }

  "renaming an index should work" in {
    val initialDataModel =
      """
        |type A @indexes(value: [
        |  { fields: ["field"] name: "A_field_idx" }
        |]) {
        |  id: ID! @id
        |  field: String
        |}
      """.stripMargin

    deploy(initialDataModel, ConnectorCapabilities(IndexesCapability))

    val dataModel =
      """
        |type A @indexes(value: [
        |  { fields: ["field"] name: "A_derp_idx" }
        |]) {
        |  id: ID! @id
        |  field: String
        |}
      """.stripMargin
    val result = deploy(dataModel, ConnectorCapabilities(IndexesCapability))
    val index  = result.table_!("A").indexes.find(_.name == "A_derp_idx").get
    val oldIndex  = result.table_!("A").indexes.find(_.name == "A_field_idx")

    oldIndex should be(None)
    index.columns.size should be(1)
    index.columns should contain("field")
  }

  def setup() = {
    val idAsString = testDependencies.projectIdEncoder.toEncodedString(name, stage)
    deployConnector.deleteProjectDatabase(idAsString).await()
    server.addProject(name, stage)
    deploy(initialDataModel, ConnectorCapabilities.empty)
    project = testDependencies.projectPersistence.load(serviceId).await.get
  }

  def deploy(dataModel: String, capabilities: ConnectorCapabilities = ConnectorCapabilities.empty): Tables = {
    val input = DeployMutationInput(
      clientMutationId = None,
      name = name,
      stage = stage,
      types = dataModel,
      dryRun = None,
      force = None,
      secrets = Vector.empty,
      functions = Vector.empty
    )
    val refreshedProject = testDependencies.projectPersistence.load(project.id).await.get
    val mutation = DeployMutation(
      args = input,
      project = refreshedProject,
      schemaInferrer = SchemaInferrer(capabilities),
      migrationStepsInferrer = MigrationStepsInferrer(),
      schemaMapper = SchemaMapper,
      migrationPersistence = testDependencies.migrationPersistence,
      projectPersistence = testDependencies.projectPersistence,
      migrator = testDependencies.migrator,
      functionValidator = testDependencies.functionValidator,
      invalidationPublisher = testDependencies.invalidationPublisher,
      capabilities = capabilities,
      clientDbQueries = deployConnector.clientDBQueries(project),
      databaseIntrospectionInferrer = EmptyDatabaseIntrospectionInferrer,
      fieldRequirements = FieldRequirementsInterface.empty,
      isActive = true
    )

    val result = mutation.execute.await
    result match {
      case MutationSuccess(result) =>
        if (result.errors.nonEmpty) {
          sys.error(s"Deploy returned unexpected errors: ${result.errors}")
        } else {
          inspect
        }
      case MutationError =>
        sys.error("Deploy returned an unexpected error")
    }
  }

  def inspect: Tables = {
    deployConnector.testFacilities.inspector.inspect(serviceId).await()
  }
}
