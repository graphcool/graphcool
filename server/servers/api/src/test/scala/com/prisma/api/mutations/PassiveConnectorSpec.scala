package com.prisma.api.mutations

import com.prisma.ConnectorTag.PostgresConnectorTag
import com.prisma.api.ApiSpecBase
import com.prisma.deploy.connector.postgres.PostgresDeployConnector
import com.prisma.shared.models.ConnectorCapability.SupportsExistingDatabasesCapability
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}

// TODO: maybe do4gr can extend those to MySQL, SQLite as well
trait PassiveConnectorSpec extends FlatSpec with Matchers with ApiSpecBase {
  val schema = "passive_test"

  override def runOnlyForConnectors   = Set(PostgresConnectorTag)
  override def runOnlyForCapabilities = Set(SupportsExistingDatabasesCapability)

  def executeOnInternalDatabase(sql: String) = {
    val connector = deployConnector.asInstanceOf[PostgresDeployConnector]
    val session   = connector.managementDatabase.database.createSession()
    val statement = session.createStatement()
    statement.execute(sql)
    session.close()
  }

}

class PassiveConnectorSpecForInlineRelations extends PassiveConnectorSpec {

  val inlineRelationSchema = s"""
                                |DROP SCHEMA IF EXISTS $schema CASCADE;
                                |CREATE SCHEMA $schema;
                                |CREATE TABLE $schema.list (
                                |  id      varchar PRIMARY KEY  -- implicit primary key constraint
                                |, name    text NOT NULL UNIQUE
                                |);
                                |
                                |CREATE TABLE $schema.user (
                                |  id      varchar PRIMARY KEY  -- implicit primary key constraint
                                |, name    text NOT NULL
                                |);
                                |
                                |CREATE TABLE $schema.todo (
                                |  id       varchar PRIMARY KEY
                                |, title     text NOT NULL UNIQUE
                                |, list_id varchar REFERENCES $schema.list (id) ON UPDATE CASCADE
                                |, user_id varchar REFERENCES $schema.user (id) ON UPDATE CASCADE
                                |);
      """.stripMargin

  lazy val inlineRelationProject = SchemaDsl.fromStringV11ForExistingDatabase(id = schema) {
    """
      | type List @db(name: "list"){
      |   id: ID! @id
      |   name: String! @unique
      |   todos: [Todo]
      | }
      |
      | type Todo @db(name: "todo"){
      |   id: ID! @id
      |   title: String! @unique
      |   list: List @db(name: "list_id")
      |   user: MyUser @db(name: "user_id")
      | }
      |
      | type MyUser @db(name: "user"){ # it's called MyUser so that the type is on the right side of the relation to ensure a bug is not there
      |   id: ID! @id
      |   name: String!
      | }
    """.stripMargin
  }

  "A Create Mutation" should "create and return item" in {
    executeOnInternalDatabase(inlineRelationSchema)
    val res = server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list name"
         |  }){ name }
         |}""".stripMargin,
      project = inlineRelationProject
    )
    res.toString should be(s"""{"data":{"createList":{"name":"the list name"}}}""")
  }

  "A Create Mutation" should "created nested items" in {
    executeOnInternalDatabase(inlineRelationSchema)
    val res = server.query(
      s"""mutation {
         |  createTodo(data: {
         |    title: "the todo"
         |    list: {
         |      create: { name: "the list" }
         |    }
         |  }){ title
         |      list{
         |          name
         |      }
         |  }
         |}""".stripMargin,
      project = inlineRelationProject
    )
    res.toString should be(s"""{"data":{"createTodo":{"title":"the todo","list":{"name":"the list"}}}}""")
  }

  "A Create Mutation" should "create nested items 2" in {
    executeOnInternalDatabase(inlineRelationSchema)
    val res = server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list"
         |    todos: {
         |      create: [{ title: "the list" }]
         |    }
         |  }){ name }
         |}""".stripMargin,
      project = inlineRelationProject
    )
    res.toString should be(s"""{"data":{"createList":{"name":"the list"}}}""")
  }

  "A Create Mutation" should "create nested items 3" in {
    executeOnInternalDatabase(inlineRelationSchema)
    val res = server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list"
         |    todos: {
         |      create: [{ title: "the todo" }]
         |    }
         |  }){ name }
         |}""".stripMargin,
      project = inlineRelationProject
    )
    res.toString should be(s"""{"data":{"createList":{"name":"the list"}}}""")

    val res2 = server.query(
      s"""mutation {
         |  updateList(
         |  where: {name:"the list"}
         |  data: {
         |    todos: {
         |      create: [{ title: "the todo 2" }]
         |    }
         |  }){ name todos{title} }
         |}""",
      project = inlineRelationProject
    )
    res2.toString should be(s"""{"data":{"updateList":{"name":"the list","todos":[{"title":"the todo"},{"title":"the todo 2"}]}}}""")

  }

  "Expanding 2 inline relations on a type" should "work" in {
    executeOnInternalDatabase(inlineRelationSchema)

    server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list"
         |    todos: {
         |      create: [{
         |         title: "the todo"
         |         user: {
         |           create: { name: "the user" }
         |         }
         |      }]
         |    }
         |  }){ name }
         |}""".stripMargin,
      project = inlineRelationProject
    )

    val res = server.query(
      s"""{
         |  todoes {
         |    title
         |    list { name }
         |    user { name }
         |  }
         |}""".stripMargin,
      project = inlineRelationProject
    )
    res should be(s"""{"data":{"todoes":[{"title":"the todo","list":{"name":"the list"},"user":{"name":"the user"}}]}}""".parseJson)
  }

  "the connector" should "support diverging names for models/tables and fields/columns" in {
    executeOnInternalDatabase(inlineRelationSchema)
    val project = SchemaDsl.fromStringV11ForExistingDatabase(id = schema) {
      """
        | type List @db(name: "list"){
        |   id: ID! @id
        |   theName: String! @db(name: "name")
        | }
      """.stripMargin
    }
    val res = server.query(
      s"""mutation {
         |  createList(data: {
         |    theName: "the list"
         |  }){ theName }
         |}""".stripMargin,
      project = project
    )
    res.toString should be(s"""{"data":{"createList":{"theName":"the list"}}}""")
  }
}

class PassiveConnectorSpecForTableRelations extends FlatSpec with PassiveConnectorSpec with Matchers with ApiSpecBase {
  val relationTableSchema =
    s"""
       |DROP SCHEMA IF EXISTS $schema CASCADE;
       |CREATE SCHEMA $schema;
       |CREATE TABLE $schema.list (
       |  id      varchar PRIMARY KEY  -- implicit primary key constraint
       |, name    text NOT NULL
       |);
       |
       |CREATE TABLE $schema.todo (
       |  id       varchar PRIMARY KEY
       |, title     text NOT NULL
       |);
       |
       |CREATE TABLE $schema.list_to_todo (
       |  list_id varchar REFERENCES $schema.list (id) ON UPDATE CASCADE ON DELETE CASCADE
       |, todo_id varchar REFERENCES $schema.todo (id) ON UPDATE CASCADE ON DELETE CASCADE
       |);
     """.stripMargin

  lazy val relationTableProject = SchemaDsl.fromStringV11ForExistingDatabase(id = schema) {
    """
      | type List @db(name: "list"){
      |   id: ID! @id
      |   name: String!
      |   todos: [Todo] @relation(name: "ListToTodo")
      | }
      |
      | type Todo @db(name: "todo"){
      |   id: ID! @id
      |   title: String!
      |   list: List @relation(name: "ListToTodo")
      | }
      |
      | type ListToTodo @relationTable @db(name: "list_to_todo") {
      |   list: List! @db(name: "list_id")
      |   todo_id: Todo!
      | }
    """.stripMargin
  }

  "A Create Mutation" should "create and return item" in {
    executeOnInternalDatabase(relationTableSchema)
    val res = server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list name"
         |  }){ name }
         |}""".stripMargin,
      project = relationTableProject
    )
    res.toString should be(s"""{"data":{"createList":{"name":"the list name"}}}""")
  }

  "A Create Mutation" should "created nested items" in {
    executeOnInternalDatabase(relationTableSchema)
    // how do we implement this? We would have to reorder in this case?
    val res = server.query(
      s"""mutation {
         |  createTodo(data: {
         |    title: "the todo"
         |    list: {
         |      create: { name: "the list" }
         |    }
         |  }){ title }
         |}""".stripMargin,
      project = relationTableProject
    )
    res.toString should be(s"""{"data":{"createTodo":{"title":"the todo"}}}""")
  }
}

class PassiveConnectorSpecForAutoGeneratedIds extends FlatSpec with PassiveConnectorSpec with Matchers with ApiSpecBase {
  val sqlSchema =
    s"""
       |DROP SCHEMA IF EXISTS $schema CASCADE;
       |CREATE SCHEMA $schema;
       |CREATE TABLE $schema.list (
       |  id      SERIAL PRIMARY KEY  -- implicit primary key constraint
       |, name    text NOT NULL
       |, foo     text
       |);
       |
       |CREATE TABLE $schema.todo (
       |  id      SERIAL PRIMARY KEY
       |, title   text NOT NULL
       |, list_id int REFERENCES $schema.list (id) ON UPDATE CASCADE
       |);
     """.stripMargin

  lazy val project = SchemaDsl.fromStringV11ForExistingDatabase(id = schema) {
    """
      | type List @db(name: "list"){
      |   id: Int! @id
      |   name: String!
      |   foo: String
      |   todos: [Todo]
      | }
      |
      | type Todo @db(name: "todo"){
      |   id: Int! @id
      |   title: String
      |   list: List @relation(link: INLINE) @db(name: "list_id")
      | }
    """.stripMargin
  }

  "A Create Mutation" should "create and return item" in {
    executeOnInternalDatabase(sqlSchema)
    val res1 = server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list name"
         |  }){ id, name }
         |}""".stripMargin,
      project = project
    )
    res1.toString should be(s"""{"data":{"createList":{"id":1,"name":"the list name"}}}""")

    val res2 = server.query(
      s"""mutation {
         |  createList(data: {
         |    name: "the list name"
         |  }){ id, name }
         |}""".stripMargin,
      project = project
    )
    res2.toString should be(s"""{"data":{"createList":{"id":2,"name":"the list name"}}}""")
  }

  "A nested Create" should "create and return the item" in {
    executeOnInternalDatabase(sqlSchema)
    val res1 = server.query(
      s"""mutation {
         |  createList(
         |    data: {
         |      name: "the list name"
         |      todos: {
         |        create: [{title: "the todo"}]
         |      }
         |    }
         |  ){
         |   id, name
         |   todos { title }
         | }
         |}""".stripMargin,
      project = project
    )
    res1.pathAsString("data.createList.todos.[0].title") should equal("the todo")
  }
}
