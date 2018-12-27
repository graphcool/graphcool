package com.prisma.shared.models

import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.Json

class MigrationStepsJsonFormatterSpec extends FlatSpec with Matchers {

  import MigrationStepsJsonFormatter._

  "CreateIndex" should "be readable" in {
    val json = Json.parse(
      """
        |{
        |  "modelName": "foo",
        |  "indexName": "herpderp"
        |}
      """.stripMargin)

    json.as[CreateIndex] should be(CreateIndex(modelName = "foo", indexName = "herpderp"))
  }

  "DeleteIndex" should "be readable" in {
    val json = Json.parse(
      """
        |{
        |  "modelName": "foo",
        |  "indexName": "herpderp"
        |}
      """.stripMargin)

    json.as[DeleteIndex] should be(DeleteIndex(modelName = "foo", indexName = "herpderp"))
  }

  "AlterIndex" should "be readable" in {
    val json = Json.parse(
      """
        |{
        |  "modelName": "foo",
        |  "oldName": "herpderp",
        |  "newName": "narf"
        |}
      """.stripMargin)

    json.as[AlterIndex] should be(AlterIndex(modelName = "foo", oldName = "herpderp", newName = "narf"))
  }

  "CreateRelation" should "be readable in the format of January 2018" in {
    val json = Json.parse("""
        |{
        |    "name": "ListToTodo",
        |    "leftModelName": "List",
        |    "rightModelName": "Todo",
        |    "discriminator": "CreateRelation"
        |  }
      """.stripMargin)

    val create = json.as[CreateRelation]
    create.name should equal("ListToTodo")
  }

  "CreateRelation" should "be readable in the format of February 2018" in {
    val json = Json.parse("""
                            |{
                            |    "name": "ListToTodo",
                            |    "leftModelName": "List",
                            |    "rightModelName": "Todo",
                            |    "modelAOnDelete": "CASCADE",
                            |    "modelBOnDelete": "SET_NULL",
                            |    "discriminator": "CreateRelation"
                            |  }
                          """.stripMargin)

    val create = json.as[CreateRelation]
    create.name should equal("ListToTodo")
  }

  "CreateRelation" should "be readable in the BROKEN format of February 2018" in {
    val json = Json.parse("""
                            |{
                            |    "name": "ListToTodo",
                            |    "modelAName": "List",
                            |    "modelBName": "Todo",
                            |    "modelAOnDelete": "CASCADE",
                            |    "modelBOnDelete": "SET_NULL",
                            |    "discriminator": "CreateRelation"
                            |  }
                          """.stripMargin)

    val create = json.as[CreateRelation]
    create.name should equal("ListToTodo")
  }

  "UpdateRelation" should "be readable in the format of January 2018" in {
    val json = Json.parse("""
                            |{
                            |    "name": "ListToTodo",
                            |    "newName": "ListToTodoNew",
                            |    "modelAId": "List",
                            |    "modelBId": "Todo",
                            |    "discriminator": "UpdateRelation"
                            |  }
                          """.stripMargin)

    val create = json.as[UpdateRelation]
    create.name should equal("ListToTodo")
    create.newName should equal(Some("ListToTodoNew"))
  }

  "UpdateRelation" should "be readable in the format of February 2018" in {
    val json = Json.parse("""
                            |{
                            |    "name": "ListToTodo",
                            |    "newName": "ListToTodoNew",
                            |    "modelAId": "List",
                            |    "modelBId": "Todo",
                            |    "modelAOnDelete": "CASCADE",
                            |    "modelBOnDelete": "SET_NULL",
                            |    "discriminator": "UpdateRelation"
                            |  }
                          """.stripMargin)

    val create = json.as[UpdateRelation]
    create.name should equal("ListToTodo")
    create.newName should equal(Some("ListToTodoNew"))
  }

  "UpdateField" should "be readable in the format of 1.8" in {
    val json = Json.parse("""
                            |{
                            |    "model": "Todo",
                            |    "name": "Field",
                            |    "newName": "NewField",
                            |    "typeName": "Todo",
                            |    "isRequired": true,
                            |    "isList": true
                            |  }
                          """.stripMargin)

    val updateField = json.as[UpdateField]
    updateField.name should equal("Field")
    updateField.newName should equal(Some("NewField"))
    updateField.model should equal("Todo")
    updateField.newModel should equal("Todo")
  }

  "UpdateField" should "be readable in the format of 1.9" in {
    val json = Json.parse("""
                            |{
                            |    "model": "Todo",
                            |    "newModel": "NewTodo",
                            |    "name": "Field",
                            |    "newName": "NewField",
                            |    "typeName": "Todo",
                            |    "isRequired": true,
                            |    "isList": true
                            |  }
                          """.stripMargin)

    val updateField = json.as[UpdateField]
    updateField.name should equal("Field")
    updateField.newName should equal(Some("NewField"))
    updateField.model should equal("Todo")
    updateField.newModel should equal("NewTodo")
  }
}
