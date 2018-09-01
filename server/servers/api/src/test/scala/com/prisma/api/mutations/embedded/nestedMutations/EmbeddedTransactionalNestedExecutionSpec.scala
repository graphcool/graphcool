package com.prisma.api.mutations.embedded.nestedMutations

import com.prisma.api.ApiSpecBase
import com.prisma.shared.models.Project
import com.prisma.shared.schema_dsl.SchemaDsl
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{FlatSpec, Matchers}

class EmbeddedTransactionalNestedExecutionSpec extends FlatSpec with Matchers with ApiSpecBase {
  override def doNotRunSuiteForMongo: Boolean = true

  //At the moment we are only inserting the inner where, the outer condition is checked separately
  //the up front check for the outer where is still needed to provide return values

  // - put a catch all handling on it in the end?
  //Test the parsing of the exception for different datatypes -> DateTime, Json problematic

  "a many to many relation" should "fail gracefully on wrong STRING where and assign error correctly and not execute partially" in {

    val outerWhere        = """"Outer Unique""""
    val innerWhere        = """"Inner Unique""""
    val falseWhere        = """"False  Where""""
    val falseWhereInError = """False  Where"""

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: String @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: String @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong INT where and assign error correctly and not execute partially" in {

    val outerWhere        = 1
    val innerWhere        = 2
    val falseWhere        = 3
    val falseWhereInError = 3

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: Int @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: Int @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong FLOAT where and assign error correctly and not execute partially" in {

    val outerWhere        = 1.0
    val innerWhere        = 2.0
    val falseWhere        = 3.0
    val falseWhereInError = 3.0

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: Float @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: Float @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong BOOLEAN = FALSE where and assign error correctly and not execute partially" in {

    val outerWhere        = true
    val innerWhere        = true
    val falseWhere        = false
    val falseWhereInError = false

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: Boolean @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: Boolean @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong BOOLEAN = TRUE where and assign error correctly and not execute partially" in {

    val outerWhere        = false
    val innerWhere        = false
    val falseWhere        = true
    val falseWhereInError = true

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: Boolean @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: Boolean @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong GRAPHQLID where and assign error correctly and not execute partially" in {

    val outerWhere        = """"Some Outer ID""""
    val innerWhere        = """"Some Inner ID""""
    val falseWhere        = """"Some False ID""""
    val falseWhereInError = "Some False ID"

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: ID @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: ID @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong ENUM where and assign error correctly and not execute partially" in {

    val outerWhere        = "A"
    val innerWhere        = "B"
    val falseWhere        = "C"
    val falseWhereInError = "C"

    val project = SchemaDsl.fromString() {
      """enum SomeEnum {
        | A
        | B
        | C
        |}
        |
        |type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: SomeEnum @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: SomeEnum @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong DateTime where and assign error correctly and not execute partially" in {
    val outerWhere        = """"2018""""
    val innerWhere        = """"2019""""
    val falseWhere        = """"2020""""
    val falseWhereInError = new DateTime("2020", DateTimeZone.UTC)

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: DateTime @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: DateTime @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many to many relation" should "fail gracefully on wrong JSON where and assign error correctly and not execute partially" in {

    val outerWhere        = """"{\"a\":\"a\"}""""
    val innerWhere        = """"{\"a\":\"b\"}""""
    val falseWhere        = """"{\"a\":\"c\"}""""
    val falseWhereInError = """{\"a\":\"c\"}"""

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: Json @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: Json @unique
        | todo: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    verifyTransactionalExecutionAndErrorMessage(outerWhere, innerWhere, falseWhere, falseWhereInError, project)
  }

  "a many2many relation" should "fail gracefully on wrong GRAPHQLID for multiple nested wheres" in {

    val outerWhere         = """"Some Outer ID""""
    val innerWhere         = """"Some Inner ID""""
    val innerWhere2        = """"Some Inner ID2""""
    val falseWhere         = """"Some False ID""""
    val falseWhere2        = """"Some False ID2""""
    val falseWhereInError  = "Some False ID"
    val falseWhereInError2 = "Some False ID2"

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: ID @unique
        | notes: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: ID @unique
        | todos: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    val createResult = server.query(
      s"""mutation {
         |  createNote(
         |    data: {
         |      outerString: "Outer String"
         |      outerUnique: $outerWhere
         |      todos: {
         |        create: [
         |        {innerString: "Inner String", innerUnique: $innerWhere},
         |        {innerString: "Inner String", innerUnique: $innerWhere2}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}""".stripMargin,
      project
    )

    server.queryThatMustFail(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerUnique: $outerWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todos: {
         |        update: [
         |        {where: { innerUnique: $innerWhere },data:{ innerString: "Changed Inner String"}},
         |        {where: { innerUnique: $falseWhere2 },data:{ innerString: "Changed Inner String"}}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """.stripMargin,
      project,
      errorCode = 3039,
      errorContains = s"No Node for the model Todo with value $falseWhereInError2 for innerUnique found."
    )

    server.query(s"""query{note(where:{outerUnique:$outerWhere}){outerString}}""", project, dataContains = s"""{"note":{"outerString":"Outer String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere2}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")

    server.queryThatMustFail(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerUnique: $outerWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todos: {
         |        update: [
         |        {where: { innerUnique: $falseWhere},data:{ innerString: "Changed Inner String"}},
         |        {where: { innerUnique: $innerWhere2 },data:{ innerString: "Changed Inner String"}}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """.stripMargin,
      project,
      errorCode = 3039,
      errorContains = s"No Node for the model Todo with value $falseWhereInError for innerUnique found."
    )

    server.query(s"""query{note(where:{outerUnique:$outerWhere}){outerString}}""", project, dataContains = s"""{"note":{"outerString":"Outer String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere2}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")
  }

  "a many2many relation" should "fail gracefully on wrong GRAPHQLID for multiple nested updates where one of them is not connected" in {

    val outerWhere  = """"Some Outer ID""""
    val innerWhere  = """"Some Inner ID""""
    val innerWhere2 = """"Some Inner ID2""""

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerUnique: ID @unique
        | note: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerUnique: ID @unique
        | todos: [Todo!]!
        |}
      """.stripMargin
    }
    database.setup(project)

    server.query(
      s"""mutation {
         |  createNote(
         |    data: {
         |      outerString: "Outer String"
         |      outerUnique: $outerWhere
         |      todos: {
         |        create: [
         |        {innerString: "Inner String", innerUnique: $innerWhere}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}""".stripMargin,
      project
    )

    server.query(s"""mutation {createTodo(data:{innerString: "Inner String", innerUnique: $innerWhere2}){id}}""".stripMargin, project)

    server.queryThatMustFail(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerUnique: $outerWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todos: {
         |        update: [
         |        {where: { innerUnique: $innerWhere },data:{ innerString: "Changed Inner String"}},
         |        {where: { innerUnique: $innerWhere2 },data:{ innerString: "Changed Inner String"}}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """.stripMargin,
      project,
      errorCode = 3041,
      errorContains =
        s"The relation TodoToNote has no node for the model Note connected to a Node for the model Todo with the value 'Some Inner ID2' for the field 'innerUnique' on your mutation path."
    )

    server.query(s"""query{note(where:{outerUnique:$outerWhere}){outerString}}""", project, dataContains = s"""{"note":{"outerString":"Outer String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere2}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")
  }

  private def verifyTransactionalExecutionAndErrorMessage(outerWhere: Any, innerWhere: Any, falseWhere: Any, falseWhereInError: Any, project: Project) = {
    server.query(
      s"""mutation {
         |  createNote(
         |    data: {
         |      outerString: "Outer String"
         |      outerUnique: $outerWhere
         |      todo: {
         |        create: {
         |         innerString: "Inner String"
         |         innerUnique: $innerWhere
         |         }
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}""".stripMargin,
      project
    )

    server.queryThatMustFail(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerUnique: $outerWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todo: {
         |        update: {
         |          where: { innerUnique: $falseWhere },
         |          data:{ innerString: "Changed Inner String" }
         |        }
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """.stripMargin,
      project,
      errorCode = 3039,
      errorContains = s"No Node for the model Todo with value $falseWhereInError for innerUnique found."
    )

    server.query(s"""query{note(where:{outerUnique:$outerWhere}){outerString}}""", project, dataContains = s"""{"note":{"outerString":"Outer String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")

    server.queryThatMustFail(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerUnique: $falseWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todo: {
         |        update: {
         |          where: { innerUnique: $innerWhere },
         |          data:{ innerString: "Changed Inner String" }
         |        }
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """.stripMargin,
      project,
      errorCode = 3039,
      errorContains = s"No Node for the model Note with value $falseWhereInError for outerUnique found."
    )

    server.query(s"""query{note(where:{outerUnique:$outerWhere}){outerString}}""", project, dataContains = s"""{"note":{"outerString":"Outer String"}}""")
    server.query(s"""query{todo(where:{innerUnique:$innerWhere}){innerString}}""", project, dataContains = s"""{"todo":{"innerString":"Inner String"}}""")
  }
}
