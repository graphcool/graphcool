package com.prisma.integration

import org.scalatest.{FlatSpec, Matchers}

class IntrospectionIntegrationSpec extends FlatSpec with Matchers with IntegrationBaseSpec {

  "Introspection" should "return same schema as deployed" in {

    val schema =
      """|type A {
         | name: String! @unique
         | value: Int
         |}""".stripMargin

    val (project, _) = setupProject(schema)

    val result = deployServer.query("""
        |{
        |  listCollections {
        |    name
        |    schema
        |  }
        |}
      """.stripMargin)

    result.toString should include(""""schema":"type A {\n  id: String!\n  createdAt: DateTime!\n  updatedAt: DateTime!\n  name: String!\n  value: Int\n}""")
  }
}
