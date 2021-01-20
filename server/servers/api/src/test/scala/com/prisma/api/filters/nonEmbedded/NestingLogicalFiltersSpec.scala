package com.prisma.api.filters.nonEmbedded

import com.prisma.ConnectorTag.{DocumentConnectorTag, RelationalConnectorTag}
import com.prisma.api.ApiSpecBase
import com.prisma.shared.models.ConnectorCapability.JoinRelationLinksCapability
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}

class NestingLogicalFiltersSpec extends FlatSpec with Matchers with ApiSpecBase {
  override def runOnlyForCapabilities = Set(JoinRelationLinksCapability)

  val project = SchemaDsl.fromStringV11() {
    connectorTag match {
      case _: RelationalConnectorTag =>
        """type Blog {
          |  id: ID! @id
          |  name: String!
          |  labels: [Label!]! @relation(name: "BlogLabels", link: TABLE)
          |  author: Author @relation(name: "BlogAuthor", link: INLINE)
          |}
          |
          |type Label {
          |  id: ID! @id
          |  name: String! @unique
          |  text: String
          |}
          |
          |type Author {
          |  id: ID! @id
          |  name: String! @unique
          |  title: String
          |}
          |
          |"""

      case _: DocumentConnectorTag =>
        """type Blog {
          |  id: ID! @id
          |  name: String!
          |  labels: [Label!]! @relation(name: "BlogLabels", link: INLINE)
          |  author: Author @relation(name: "BlogAuthor", link: INLINE)
          |}
          |
          |type Label {
          |  id: ID! @id
          |  name: String! @unique
          |  text: String!
          |}
          |
          |type Author {
          |  id: ID! @id
          |  name: String! @unique
          |  title: String
          |}
          |
          |"""
    }
  }

  "Filter Queries along self relations" should "succeed with one level " in {
    database.setup(project)

    server.query(
      """mutation {
                   |  l1: createLabel(data:{
                   |    name: "x",
                   |    text: "a"
                   |  }) {
                   |    name
                   |  }
                   |  l2: createLabel(data:{
                   |    name: "y",
                   |    text: "b"
                   |  }) {
                   |    name
                   |  }
                   |}""",
      project
    )

    val authorId = server
      .query(
        """mutation {
                   |  createBlog(data:{
                   |    name: "blog"
                   |    labels: {
                   |      connect: [
                   |        { name: "x" },
                   |        { name: "y" }
                   |      ]
                   |    }
                   |    author:{
                   |      create:{
                   |        name: "test",
                   |        title: "a"
                   |      }
                   |    }
                   |  }) {
                   |    name
                   |    labels {
                   |      name
                   |      id
                   |    }
                   |    author{
                   |      id
                   |    }
                   |  }
                   |}""",
        project
      )
      .pathAsString("data.createBlog.author.id")

    server.query(
      s"""query {
         |  q1: blogs(where: {
         |  AND:[
         |    {author:{
         |      name_starts_with: "te"
         |    }},
         |    {author:{
         |      name_ends_with: "st"
         |    }}]
         |  }) {
         |    name
         |  }
         |}""",
      project,
      dataContains = """{"q1":[{"name":"blog"}]}"""
    )

    server.query(
      s"""query {
         |  q1: blogs(where: {
         |  AND:[
         |    {author:{
         |      name_starts_with: "te"
         |    }},
         |    {author:{
         |      name_ends_with: "st"
         |    }},
         |    {AND:{author:{
         |      name_contains: "t"
         |    }}}]
         |  }) {
         |    name
         |  }
         |}""",
      project,
      dataContains = """{"q1":[{"name":"blog"}]}"""
    )

    server.query(
      s"""query {
         |  q1: blogs(where: {
         |  AND:[
         |    {author:{
         |      name: "test"
         |    }},
         |    {author:{
         |      title: "a"
         |    }}]
         |  }) {
         |    name
         |  }
         |}""",
      project,
      dataContains = """{"q1":[{"name":"blog"}]}"""
    )

    server.query(
      """query {
                   |  q1: blogs(where: {
                   |    labels_some:{
                   |      name: "x"
                   |    },
                   |    AND:{
                   |      labels_some:{
                   |        name: "y"
                   |      }
                   |    }
                   |  }) {
                   |    name
                   |  }
                   |}""",
      project,
      dataContains = """{"q1":[{"name":"blog"}]}"""
    )

    server.query(
      """query {
        |  q1: blogs(where: {
        |    labels_some:{
        |      name: "x"
        |    },
        |    AND:{
        |      labels_some:{
        |        name_starts_with: "y"
        |      }
        |    }
        |  }) {
        |    name
        |  }
        |}""",
      project,
      dataContains = """{"q1":[{"name":"blog"}]}"""
    )

    server.query(
      s"""query {
        |  q1: blogs(where: {
        |    author:{
        |      id: "$authorId"
        |    }
        |  }) {
        |    name
        |  }
        |}""",
      project,
      dataContains = """{"q1":[{"name":"blog"}]}"""
    )

  }
}
