package com.prisma.api.queries

import com.prisma.api.{ApiSpecBase, TestDataModels}
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}
import com.prisma.IgnoreSQLite
import com.prisma.shared.models.ConnectorCapability
import com.prisma.shared.models.ConnectorCapability.Prisma2Capability

class MultiItemConnectionQuerySpec extends FlatSpec with Matchers with ApiSpecBase {

  override def doNotRunForCapabilities: Set[ConnectorCapability] = Set(Prisma2Capability)

  val project = SchemaDsl.fromStringV11() {
    """type Todo {
      |  id: ID! @id
      |  title: String!
      |}
    """.stripMargin
  }

  "the connection query" should "return empty edges" in {
    database.setup(project)

    val result = server.query(
      s"""{
         |  todoesConnection{
         |    edges {
         |      node {
         |        title
         |      }
         |    }
         |  }
         |}""".stripMargin,
      project
    )

    result.toString should equal("""{"data":{"todoesConnection":{"edges":[]}}}""")
  }

  "the connection query" should "return single node" in {
    database.setup(project)

    val title = "Hello World!"
    val id = server
      .query(
        s"""mutation {
           |  createTodo(data: {title: "$title"}) {
           |    id
           |  }
           |}""".stripMargin,
        project
      )
      .pathAsString("data.createTodo.id")

    val result = server.query(
      s"""{
         |  todoesConnection{
         |    edges {
         |      node {
         |        title
         |      }
         |    }
         |  }
         |}""".stripMargin,
      project
    )

    result.toString should equal("""{"data":{"todoesConnection":{"edges":[{"node":{"title":"Hello World!"}}]}}}""")
  }

  "the connection query" should "filter by any field" in {
    database.setup(project)

    val title = "Hello World!"
    val id = server
      .query(
        s"""mutation {
           |  createTodo(data: {title: "$title"}) {
           |    id
           |  }
           |}""".stripMargin,
        project
      )
      .pathAsString("data.createTodo.id")

    server
      .query(
        s"""{
         |  todoesConnection(where: {title: "INVALID"}){
         |    edges {
         |      node {
         |        title
         |      }
         |    }
         |  }
         |}""".stripMargin,
        project
      )
      .toString should equal("""{"data":{"todoesConnection":{"edges":[]}}}""")

    server
      .query(
        s"""{
         |  todoesConnection(where: {title: "${title}"}){
         |    edges {
         |      node {
         |        title
         |      }
         |    }
         |  }
         |}""".stripMargin,
        project
      )
      .toString should equal("""{"data":{"todoesConnection":{"edges":[{"node":{"title":"Hello World!"}}]}}}""")
  }

  "the connection query" should "work when using cursors" taggedAs (IgnoreSQLite) in {
    val datamodels = {
      val dm1 =
        """type User {
                id: ID! @id
                name: String
                following: [User!]! @relation(name: "UserToFollow", link: INLINE)
                followers: [User!]! @relation(name: "UserToFollow")
              }"""

      val dm2 =
        """type User {
                id: ID! @id
                name: String
                following: [User!]! @relation(name: "UserToFollow")
                followers: [User!]! @relation(name: "UserToFollow", link: INLINE)
              }"""

      val dm3 =
        """type User {
          |  id: ID! @id
          |  name: String
          |  following: [User!]! @relation(name: "UserToFollow", link: TABLE)
          |  followers: [User!]! @relation(name: "UserToFollow")
          |}"""

      TestDataModels(mongo = Vector(dm1, dm2), sql = Vector(dm3))
    }
    datamodels.testV11 { project =>
      val a = server.query(s"""mutation{createUser(data:{name: "a", followers:{create:[{name:"b"}, {name:"c"}, {name:"x"}]}}){id}}""", project)
      val d = server.query(s"""mutation{createUser(data:{name: "d", followers:{create:[{name:"e"}, {name:"f"}, {name:"x"}]}}){id}}""", project)
      val g = server.query(s"""mutation{createUser(data:{name: "g", followers:{create:[{name:"h"}, {name:"i"}, {name:"x"}]}}){id}}""", project)
      val k = server.query(s"""mutation{createUser(data:{name: "k", followers:{create:[{name:"l"}, {name:"m"}, {name:"x"}]}}){id}}""", project)

      val result = server.query(
        s"""{
           |  usersConnection(where: {
           |    followers_some: {
           |      name: "x"
           |    },
           |  }, first: 2, after: "${a.pathAsString("data.createUser.id")}") {
           |   aggregate {
           |	    count
           |    }
           |    edges {
           |      node {
           |        name
           |      }
           |    }
           |  }
           |}""".stripMargin,
        project
      )

      result.toString should be("""{"data":{"usersConnection":{"aggregate":{"count":2},"edges":[{"node":{"name":"d"}},{"node":{"name":"g"}}]}}}""")

    }
  }

  "the connection query" should "work when not using cursors" taggedAs (IgnoreSQLite) in {
    val datamodels = {
      val dm1 =
        """type User {
          |  id: ID! @id
          |  name: String
          |  company: Company @relation(link: INLINE)
          |}
          |
          |type Company {
          |  id: ID! @id
          |  name: String,
          |  members: [User!]!
          |}"""

      val dm2 =
        """type User {
          |  id: ID! @id
          |  name: String
          |  company: Company
          |}
          |
          |type Company {
          |  id: ID! @id
          |  name: String,
          |  members: [User!]! @relation(link: INLINE)
          |}"""

      val dm3 =
        """type User {
          |  id: ID! @id
          |  name: String
          |  company: Company @relation(link: TABLE)
          |}
          |
          |type Company {
          |  id: ID! @id
          |  name: String,
          |  members: [User!]!
          |}"""

      TestDataModels(mongo = Vector(dm1, dm2), sql = Vector(dm3))
    }

    datamodels.testV11 { project =>
      val a = server.query(s"""mutation{createUser(data:{name: "a", company:{create:{name:"b"}}}){id, company{id}}}""", project)
      val d = server.query(s"""mutation{createUser(data:{name: "d", company:{create:{name:"e"}}}){id, company{id}}}""", project)
      val g = server.query(s"""mutation{createUser(data:{name: "g", company:{create:{name:"h"}}}){id, company{id}}}""", project)
      val k = server.query(s"""mutation{createUser(data:{name: "k", company:{create:{name:"l"}}}){id, company{id}}}""", project)

      val aCompanyId = a.pathAsString("data.createUser.company.id")

      val result = server.query(
        s"""{
          |  usersConnection(where: {
          |    company: {
          |      id: "$aCompanyId"
          |    }
          |  }) {
          |    edges {
          |      node {
          |        name
          |        company {
          |          name
          |        }
          |      }
          |    }
          |    aggregate {
          |      count
          |    }
          |  }
          |}""".stripMargin,
        project
      )

      result.toString should be("""{"data":{"usersConnection":{"edges":[{"node":{"name":"a","company":{"name":"b"}}}],"aggregate":{"count":1}}}}""")
    }
  }

}
