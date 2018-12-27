package com.prisma.deploy.migration.inference

import com.prisma.deploy.connector.InferredTables
import com.prisma.deploy.migration.validation.LegacyDataModelValidator
import com.prisma.deploy.specutils.DeploySpecBase
import com.prisma.shared.models.ConnectorCapability.{LegacyDataModelCapability, MigrationsCapability}
import com.prisma.shared.models.Manifestations.{EmbeddedRelationLink, FieldManifestation, ModelManifestation, RelationTable}
import com.prisma.shared.models._
import com.prisma.shared.schema_dsl.{SchemaDsl, TestProject}
import org.scalatest.{Matchers, WordSpec}

class LegacySchemaInfererSpec extends WordSpec with Matchers with DeploySpecBase {
  val emptyProject = TestProject.empty

  "if a given relation does not exist yet, the inferer" should {
    "infer relations with the given name if a relation directive is provided on both sides" in {
      val types =
        """
          |type Todo {
          |  comments: [Comment] @relation(name:"MyNameForTodoToComments")
          |}
          |
          |type Comment {
          |  todo: Todo! @relation(name:"MyNameForTodoToComments")
          |}
        """.stripMargin.trim()
      val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

      val relation = schema.getRelationByName_!("MyNameForTodoToComments")
      relation.modelAName should equal("Comment")
      relation.modelBName should equal("Todo")
    }

    "infer relations with the given name if a relation directive is provided on both sides and there are two relations between models" in {
      val types =
        """
          |type User {
          |  calls: [Call] @relation(name: "CallRequester")
          |  calls_member: [Call] @relation(name: "CallMembers")
          |}
          |type Call {
          |  created_by: User! @relation(name: "CallRequester")
          |  members: [User] @relation(name: "CallMembers")
          |}""".stripMargin.trim()
      val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

      val relation = schema.getRelationByName_!("CallRequester")
      relation.modelAName should equal("Call")
      relation.modelBName should equal("User")

      val relation2 = schema.getRelationByName_!("CallMembers")
      relation2.modelAName should equal("Call")
      relation2.modelBName should equal("User")
    }

    "infer relations with provided name if only one relation directive is given" in {
      val types =
        """
          |type Todo {
          |  comments: [Comment] @relation(name:"MyRelationName")
          |}
          |
          |type Comment {
          |  todo: Todo!
          |}
        """.stripMargin.trim()
      val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

      schema.relations should have(size(1))
      val relation = schema.getRelationByName_!("MyRelationName")
      relation.modelAName should equal("Comment")
      relation.modelBName should equal("Todo")
    }

    "infer relations with an auto generated name if no relation directive is given" in {
      val types =
        """
          |type Todo {
          |  comments: [Comment]
          |}
          |
          |type Comment {
          |  todo: Todo!
          |}
        """.stripMargin.trim()
      val schema = infer(emptyProject.schema, types, capabilities = Set.empty)
      schema.relations.foreach(println(_))

      val relation = schema.getRelationByName_!("CommentToTodo")
      relation.modelAName should equal("Comment")
      relation.modelBName should equal("Todo")

      val field1 = schema.getModelByName_!("Todo").getRelationFieldByName_!("comments")
      field1.isList should be(true)
      field1.relation should be(relation)

      val field2 = schema.getModelByName_!("Comment").getRelationFieldByName_!("todo")
      field2.isList should be(false)
      field2.relation should be(relation)
    }
  }

  "if a given relation does already exist, the inferer" should {
    val project = SchemaDsl.fromBuilder { schema =>
      val comment = schema.model("Comment")
      schema.model("Todo").oneToManyRelation("comments", "todo", comment, relationName = Some("CommentToTodo"))
    }

    "infer the existing relation and update it accordingly when the type names change" in {
      val types =
        """
          |type TodoNew {
          |  comments: [CommentNew]
          |}
          |
          |type CommentNew {
          |  todo: TodoNew!
          |}
        """.stripMargin

      val renames = SchemaMapping(
        models = Vector(
          Mapping(previous = "Todo", next = "TodoNew"),
          Mapping(previous = "Comment", next = "CommentNew")
        )
      )

      val newSchema = infer(project.schema, types, renames, capabilities = Set.empty)
      newSchema.relations.foreach(println(_))

      val relation = newSchema.getRelationByName_!("CommentNewToTodoNew")
      relation.modelAName should be("TodoNew")
      relation.modelBName should be("CommentNew")

      val field1 = newSchema.getModelByName_!("TodoNew").getRelationFieldByName_!("comments")
      field1.isList should be(true)
      field1.relation should be(relation)

      val field2 = newSchema.getModelByName_!("CommentNew").getRelationFieldByName_!("todo")
      field2.isList should be(false)
      field2.relation should be(relation)
    }

    "infer the existing relation although the type and field names changed" in {
      val types =
        """
          |type TodoNew {
          |  commentsNew: [CommentNew]
          |}
          |
          |type CommentNew {
          |  todoNew: TodoNew!
          |}
        """.stripMargin

      val renames = SchemaMapping(
        models = Vector(
          Mapping(previous = "Todo", next = "TodoNew"),
          Mapping(previous = "Comment", next = "CommentNew")
        ),
        fields = Vector(
          FieldMapping(previousModel = "Todo", previousField = "comments", nextModel = "TodoNew", nextField = "commentsNew"),
          FieldMapping(previousModel = "Comment", previousField = "todo", nextModel = "CommentNew", nextField = "todoNew")
        )
      )

      val newSchema = infer(project.schema, types, renames, capabilities = Set.empty)
      newSchema.relations.foreach(println(_))

      val relation = newSchema.getRelationByName_!("CommentNewToTodoNew")
      relation.modelAName should be("TodoNew")
      relation.modelBName should be("CommentNew")

      val field1 = newSchema.getModelByName_!("TodoNew").getRelationFieldByName_!("commentsNew")
      field1.isList should be(true)
      field1.relation should be(relation)

      val field2 = newSchema.getModelByName_!("CommentNew").getRelationFieldByName_!("todoNew")
      field2.isList should be(false)
      field2.relation should be(relation)
    }
  }

  "if a model already exists and it gets renamed, the inferrer" should {
    "infer the next model with the stable identifier of the existing model" in {
      val project = SchemaDsl.fromBuilder { schema =>
        schema.model("Todo").field("title", _.String)
      }
      val types =
        """
          |type TodoNew {
          |  title: String
          |}
        """.stripMargin

      val renames = SchemaMapping(
        models = Vector(
          Mapping(previous = "Todo", next = "TodoNew")
        )
      )

      val newSchema = infer(project.schema, types, renames, capabilities = Set.empty)

      val previousModel = project.schema.getModelByName_!("Todo")
      val nextModel     = newSchema.getModelByName_!("TodoNew")

      previousModel.stableIdentifier should equal(nextModel.stableIdentifier)
    }
  }

  "For self-relations the inferer" should {
    "assign fieldA to the field with the lower lexicographic order" in {
      val types =
        """|type Technology {
           |  name: String! @unique
           |  childTechnologies: [Technology] @relation(name: "ChildTechnologies")
           |  parentTechnologies: [Technology] @relation(name: "ChildTechnologies")
           |}""".stripMargin.trim()
      val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

      schema.relations should have(size(1))
      val relation = schema.getRelationByName_!("ChildTechnologies")
      relation.modelAName should equal("Technology")
      relation.modelBName should equal("Technology")
      relation.modelAField.name should be("childTechnologies")
      relation.modelBField.name should be("parentTechnologies")

    }

    "keep assignments after renames" in {
      val types =
        """|type Technology {
           |  name: String! @unique
           |  childTechnologies: [Technology] @relation(name: "ChildTechnologies")
           |  parentTechnologies: [Technology] @relation(name: "ChildTechnologies")
           |}""".stripMargin.trim()
      val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

      schema.relations should have(size(1))
      val relation = schema.getRelationByName_!("ChildTechnologies")
      relation.modelAName should equal("Technology")
      relation.modelBName should equal("Technology")
      relation.modelAField.name should be("childTechnologies")
      relation.modelBField.name should be("parentTechnologies")

      val newTypes =
        """|type NewTechnology {
           |  name: String! @unique
           |  xTechnologies: [NewTechnology] @relation(name: "ChildTechnologies")
           |  parentTechnologies: [NewTechnology] @relation(name: "ChildTechnologies")
           |}""".stripMargin.trim()

      val renames = SchemaMapping(
        models = Vector(Mapping(previous = "Technology", next = "NewTechnology")),
        fields =
          Vector(FieldMapping(previousModel = "Technology", previousField = "childTechnologies", nextModel = "NewTechnology", nextField = "xTechnologies"))
      )

      val newSchema = infer(schema, newTypes, renames, capabilities = Set.empty)
      newSchema.relations.foreach(println(_))

      val newRelation = newSchema.getRelationByName_!("ChildTechnologies")
      newRelation.modelAName should be("NewTechnology")
      newRelation.modelBName should be("NewTechnology")

      val field1 = newSchema.getModelByName_!("NewTechnology").getRelationFieldByName_!("xTechnologies")
      field1.relation should be(newRelation)
      field1.relationSide.toString should be("A")

      val field2 = newSchema.getModelByName_!("NewTechnology").getRelationFieldByName_!("parentTechnologies")
      field2.relation should be(newRelation)
      field2.relationSide.toString should be("B")
    }
  }

  "repair invalid assignments" in {
    val types =
      """|type Technology {
         |  name: String! @unique
         |  childTechnologies: [Technology] @relation(name: "ChildTechnologies")
         |  parentTechnologies: [Technology] @relation(name: "ChildTechnologies")
         |}""".stripMargin.trim()
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    schema.relations should have(size(1))
    val relation = schema.getRelationByName_!("ChildTechnologies")
    relation.modelAName should equal("Technology")
    relation.modelBName should equal("Technology")
    relation.modelAField.name should be("childTechnologies")
    relation.modelBField.name should be("parentTechnologies")

    val techModel   = schema.models.head
    val parentField = techModel.getFieldByName_!("parentTechnologies")

    val updatedModel = techModel.copy(
      fieldTemplates = techModel.fields.filter(_ != parentField).map(_.template) :+ parentField.template.copy(relationSide = Some(RelationSide.A))
    )
    val invalidSchema = schema.copy(modelTemplates = List(updatedModel))

    val newSchema = infer(invalidSchema, types, capabilities = Set.empty)
    newSchema.relations.foreach(println(_))

    val newRelation = newSchema.getRelationByName_!("ChildTechnologies")
    newRelation.modelAName should be("Technology")
    newRelation.modelBName should be("Technology")

    val field1 = newSchema.getModelByName_!("Technology").getRelationFieldByName_!("childTechnologies")
    field1.relation should be(newRelation)
    field1.relationSide.toString should be("A")

    val field2 = newSchema.getModelByName_!("Technology").getRelationFieldByName_!("parentTechnologies")
    field2.relation should be(newRelation)
    field2.relationSide.toString should be("B")
  }

  "handle optional backrelations" in {
    val types =
      """|type Technology {
         |  name: String! @unique
         |  childTechnologies: [Technology]
         |}""".stripMargin.trim()
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    schema.relations should have(size(1))
    val relation = schema.relations.head
    relation.modelAName should equal("Technology")
    relation.modelBName should equal("Technology")
    relation.modelAField.name should be("childTechnologies")
  }

  "handle database manifestations for models" in {
    val types =
      """|type Todo @pgTable(name:"todo_table"){
         |  name: String!
         |}""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    val model = schema.getModelByName_!("Todo")
    model.manifestation should equal(Some(ModelManifestation("todo_table")))
  }

  "handle pg database manifestations for fields" in {
    val types =
      """|type Todo {
         |  name: String! @pgColumn(name: "my_name_column")
         |}""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    val field = schema.getModelByName_!("Todo").getScalarFieldByName_!("name")
    field.manifestation should equal(Some(FieldManifestation("my_name_column")))
  }

  "handle pg relation table manifestations" ignore {
    val types =
      """|type Todo {
         |  name: String!
         |}
         |
         |type List {
         |  todos: [Todo] @relationTable(table: "list_to_todo", relationColumn: "list_id", targetColumn: "todo_id")
         |}""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    val relation = schema.getModelByName_!("List").getRelationFieldByName_!("todos").relation
    // assert model ids to make sure that the generated manifestation refers to the right modelAColumn/modelBColumn
    relation.modelAName should equal("List")
    relation.modelBName should equal("Todo")

    val expectedManifestation = RelationTable(table = "list_to_todo", modelAColumn = "list_id", modelBColumn = "todo_id")
    relation.manifestation should equal(Some(expectedManifestation))
  }

  "handle pg inline relation manifestations" ignore {
    val types =
      """
         |type List {
         |  todos: [Todo]
         |}
         |
         |type Todo {
         |  name: String!
         |  list: List @pgRelation(column: "list_id")
         |}
         |""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    val relation = schema.getModelByName_!("List").getRelationFieldByName_!("todos").relation

    val expectedManifestation = EmbeddedRelationLink(inTableOfModelName = "Todo", referencingColumn = "list_id")
    relation.manifestation should equal(Some(expectedManifestation))
  }

  "handle mongo inline relation manifestations" ignore {
    val types =
      """
        |type List {
        |  todos: [Todo]
        |}
        |
        |type Todo {
        |  name: String!
        |  list: List @mongoRelation(field: "list_id")
        |}
        |""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    val relation = schema.getModelByName_!("List").getRelationFieldByName_!("todos").relation

    val expectedManifestation = EmbeddedRelationLink(inTableOfModelName = "Todo", referencingColumn = "list_id")
    relation.manifestation should equal(Some(expectedManifestation))
  }

  "add hidden reserved fields if isActive is true" in {
    val types =
      """|type Todo {
         |  name: String!
         |}""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set(MigrationsCapability))

    val model = schema.getModelByName_!("Todo")
    model.fields should have(size(4))
    model.fields.map(_.name) should equal(List("name", "id", "updatedAt", "createdAt"))
  }

  "do not add hidden reserved fields if isActive is false" in {
    val types =
      """|type Todo {
         |  name: String!
         |}""".stripMargin
    val schema = infer(emptyProject.schema, types, capabilities = Set.empty)

    val model = schema.getModelByName_!("Todo")
    model.fields should have(size(1))
    model.fields.map(_.name) should equal(List("name"))
  }

  "should not blow up when no @pgRelation is used and inferred tables is empty" in {
    val types =
      """|type Todo {
         |  name: String!
         |  comments: [Comment]
         |}
         |type Comment {
         |  text: String
         |}
         |""".stripMargin
    val schema        = infer(emptyProject.schema, types, capabilities = Set.empty)
    val relationField = schema.getModelByName_!("Todo").getRelationFieldByName_!("comments")
    relationField.relation.manifestation should be(None)
  }

  def infer(schema: Schema, types: String, mapping: SchemaMapping = SchemaMapping.empty, capabilities: Set[ConnectorCapability]): Schema = {
    val actualCapabilities = ConnectorCapabilities(capabilities ++ Set(LegacyDataModelCapability))
    val validator = LegacyDataModelValidator(
      types,
      LegacyDataModelValidator.directiveRequirements,
      deployConnector.fieldRequirements,
      actualCapabilities
    )

    val prismaSdl = validator.generateSDL

    SchemaInferrer(actualCapabilities).infer(schema, mapping, prismaSdl, InferredTables.empty)

  }
}
