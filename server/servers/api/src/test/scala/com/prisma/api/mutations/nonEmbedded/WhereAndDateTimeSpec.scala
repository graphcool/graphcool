package com.prisma.api.mutations.nonEmbedded

import com.prisma.api.ApiSpecBase
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}

class WhereAndDateTimeSpec extends FlatSpec with Matchers with ApiSpecBase {
  override def doNotRunForPrototypes: Boolean = true

  "Using the same input in an update using where as used during creation of the item" should "work" in {

    val outerWhere = """"2018-12-05T12:34:23.000Z""""
    val innerWhere = """"2019-12-05T12:34:23.000Z""""

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerDateTime: DateTime @unique
        | notes: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerDateTime: DateTime @unique
        | todos: [Todo!]!
      """.stripMargin
    }
    database.setup(project)

    server.query(
      s"""mutation {
         |  createNote(
         |    data: {
         |      outerString: "Outer String"
         |      outerDateTime: $outerWhere
         |      todos: {
         |        create: [
         |        {innerString: "Inner String", innerDateTime: $innerWhere}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}""".stripMargin,
      project
    )

    server.query(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerDateTime: $outerWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todos: {
         |        update: [{where: { innerDateTime: $innerWhere },data:{ innerString: "Changed Inner String"}}]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """,
      project
    )

    server.query(
      s"""query{note(where:{outerDateTime:$outerWhere}){outerString, outerDateTime}}""",
      project,
      dataContains = """{"note":{"outerString":"Changed Outer String","outerDateTime":"2018-12-05T12:34:23.000Z"}}"""
    )
    server.query(
      s"""query{todo(where:{innerDateTime:$innerWhere}){innerString, innerDateTime}}""",
      project,
      dataContains = """{"todo":{"innerString":"Changed Inner String","innerDateTime":"2019-12-05T12:34:23.000Z"}}"""
    )
  }

  "Using the same input in an update using where as used during creation of the item" should "work 2" in {

    val outerWhere = """"2018-01-03T11:27:38+00:00""""
    val innerWhere = """"2018-01-03T11:27:38+00:00""""

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | innerString: String!
        | innerDateTime: DateTime @unique
        | notes: [Note!]!
        |}
        |
        |type Note {
        | id: ID! @unique
        | outerString: String
        | outerDateTime: DateTime @unique
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
         |      outerDateTime: $outerWhere
         |      todos: {
         |        create: [
         |        {innerString: "Inner String", innerDateTime: $innerWhere}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}""".stripMargin,
      project
    )

    server.query(
      s"""
         |mutation {
         |  updateNote(
         |    where: { outerDateTime: $outerWhere }
         |    data: {
         |      outerString: "Changed Outer String"
         |      todos: {
         |        update: [
         |        {where: { innerDateTime: $innerWhere },data:{ innerString: "Changed Inner String"}}
         |        ]
         |      }
         |    }
         |  ){
         |    id
         |  }
         |}
      """.stripMargin,
      project
    )

    server.query(s"""query{note(where:{outerDateTime:$outerWhere}){outerString}}""",
                 project,
                 dataContains = s"""{"note":{"outerString":"Changed Outer String"}}""")
    server.query(s"""query{todo(where:{innerDateTime:$innerWhere}){innerString}}""",
                 project,
                 dataContains = s"""{"todo":{"innerString":"Changed Inner String"}}""")
  }

}
