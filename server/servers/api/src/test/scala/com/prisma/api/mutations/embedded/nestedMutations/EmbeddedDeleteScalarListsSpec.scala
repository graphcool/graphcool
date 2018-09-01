package com.prisma.api.mutations.embedded.nestedMutations

import com.prisma.IgnoreMongo
import com.prisma.api.ApiSpecBase
import com.prisma.shared.models.Project
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}

class EmbeddedDeleteScalarListsSpec extends FlatSpec with Matchers with ApiSpecBase {

  override def doNotRunSuiteForMongo: Boolean  = true
  override def runSuiteOnlyForActiveConnectors = true

  "A toplevel delete  mutation" should "also delete ListTable entries" in {

    val project: Project = SchemaDsl.fromString() {
      """type TestModel {
        | id: ID! @unique
        | name: String! @unique
        | list: [Int!]!
        |}"""
    }

    database.setup(project)

    server.query(
      """mutation {
        |  createTestModel(
        |    data: { name: "test", list: {set: [1,2,3]} }
        |  ){
        |    name
        |    list
        |  }
        |}
      """,
      project
    )

    server.query("""mutation{deleteTestModel(where:{name:"test" }){name}}""", project)

    server.query("""query{testModels{name}}""", project).toString() should be("""{"data":{"testModels":[]}}""")
  }

  "A nested delete  mutation" should "also delete ListTable entries" in {

    val project: Project = SchemaDsl.fromString() {
      """type Top {
        | id: ID! @unique
        | name: String! @unique
        | bottom: Bottom
        |}
        |
        |type Bottom {
        | id: ID! @unique
        | name: String! @unique
        | list: [Int!]!
        |}"""
    }

    database.setup(project)

    server.query(
      """mutation {
        |  createTop(
        |    data: { name: "test", bottom: {create: {name: "test2", list: {set: [1,2,3]}} }}
        |  ){
        |    name
        |    bottom{name, list} 
        |  }
        |}
      """,
      project
    )

    val res = server.query("""mutation{updateTop(where:{name:"test" }data: {bottom: {delete: true}}){name, bottom{name}}}""", project)

    res.toString should be("""{"data":{"updateTop":{"name":"test","bottom":null}}}""")
  }

  "A delete Many  mutation" should "also delete ListTable entries" taggedAs (IgnoreMongo) in {

    val project: Project = SchemaDsl.fromString() {
      """type Top {
        | id: ID! @unique
        | name: String! @unique
        | list: [Int!]!
        |}"""
    }

    database.setup(project)

    server.query("""mutation {createTop(data: { name: "test", list: {set: [1,2,3]}}){name, list}}""", project)
    server.query("""mutation {createTop(data: { name: "test2", list: {set: [1,2,3]}}){name, list}}""", project)
    server.query("""mutation {createTop(data: { name: "test3", list: {set: [1,2,3]}}){name, list}}""", project)

    val res = server.query("""mutation{deleteManyTops(where:{name_contains:"2" }){count}}""", project)

    res.toString should be("""{"data":{"deleteManyTops":{"count":1}}}""")
  }

  "A cascading delete  mutation" should "also delete ListTable entries" in {

    val project: Project = SchemaDsl.fromString() {
      """type Top {
        | id: ID! @unique
        | name: String! @unique
        | bottom: Bottom @relation(name: "Test", onDelete: CASCADE)
        |}
        |
        |type Bottom {
        | id: ID! @unique
        | name: String! @unique
        | list: [Int!]!
        | top: Top! @relation(name: "Test")
        |}"""
    }

    database.setup(project)

    server.query(
      """mutation {
        |  createTop(
        |    data: { name: "test", bottom: {create: {name: "test2", list: {set: [1,2,3]}} }}
        |  ){
        |    name
        |    bottom{name, list}
        |  }
        |}
      """,
      project
    )

    server.query("""mutation{deleteTop(where:{name:"test"}){name}}""", project)

    server.query("""query{tops{name}}""", project).toString() should be("""{"data":{"tops":[]}}""")
    server.query("""query{bottoms{name}}""", project).toString() should be("""{"data":{"bottoms":[]}}""")

  }
}
