package com.prisma.api.mutations.nonEmbedded

import com.prisma.api.ApiSpecBase
import com.prisma.api.connector.ApiConnectorCapability.JoinRelationsCapability
import com.prisma.shared.models.Project
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}

class RelationDesignSpec extends FlatSpec with Matchers with ApiSpecBase {

  override def runOnlyForCapabilities = Set(JoinRelationsCapability)

  "Deleting a parent node" should "remove it from the relation and delete the relay id" in {

    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | uTodo: String @unique
        | list: List
        |}
        |
        |type List {
        | id: ID! @unique
        | uList: String @unique
        | todo: Todo
        |}
      """.stripMargin
    }

    database.setup(project)

    server.query(s"""mutation {createList(data: {uList: "A", todo : { create: {uTodo: "B"}}}){id}}""", project)

    val result = server.query(s"""query{lists {uList, todo {uTodo}}}""", project)
    result.toString should equal("""{"data":{"lists":[{"uList":"A","todo":{"uTodo":"B"}}]}}""")

    server.query(s"""query{todoes {uTodo}}""", project).toString should be("""{"data":{"todoes":[{"uTodo":"B"}]}}""")

    countItems(project, "lists") should be(1)
    countItems(project, "todoes") should be(1)

    server.query(s"""mutation{deleteList(where: {uList:"A"}){id}}""", project)

    countItems(project, "lists") should be(0)
    ifConnectorIsActive { dataResolver(project).countByTable("_RelayId").await should be(1) }
  }

  "Deleting a child node" should "remove it from the relation and delete the relay id" in {
    val project = SchemaDsl.fromString() {
      """type Todo {
        | id: ID! @unique
        | uTodo: String @unique
        | list: List
        |}
        |
        |type List {
        | id: ID! @unique
        | uList: String @unique
        | todo: Todo
        |}
      """.stripMargin
    }

    database.setup(project)

    server.query(s"""mutation {createList(data: {uList: "A", todo : { create: {uTodo: "B"}}}){id}}""", project)

    val result = server.query(s"""query{lists {uList, todo {uTodo}}}""", project)
    result.toString should equal("""{"data":{"lists":[{"uList":"A","todo":{"uTodo":"B"}}]}}""")

    server.query(s"""query{todoes {uTodo}}""", project).toString should be("""{"data":{"todoes":[{"uTodo":"B"}]}}""")

    countItems(project, "lists") should be(1)
    countItems(project, "todoes") should be(1)

    server.query(s"""mutation{deleteTodo(where: {uTodo:"B"}){id}}""", project)

    countItems(project, "todoes") should be(0)
    ifConnectorIsActive { dataResolver(project).countByTable("_RelayId").await should be(1) }
  }

  def countItems(project: Project, name: String): Int = {
    server.query(s"""query{$name{id}}""", project).pathAsSeq(s"data.$name").length
  }

}
