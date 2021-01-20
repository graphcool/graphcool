package com.prisma.api.mutations.embedded

import com.prisma.api.ApiSpecBase
import com.prisma.shared.models.Project
import com.prisma.shared.schema_dsl.SchemaDsl
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class MongoFilterPerformanceSpec extends FlatSpec with Matchers with ApiSpecBase {

  override def doNotRun: Boolean = true

  "Testing a query that uses the aggregation framework" should "work" in {
    val project = SchemaDsl.fromStringV11() {
      """
        |type User {
        |  id: ID! @id
        |  a: String
        |  b: String
        |  c: String
        |  d: Int
        |  e: Float
        |  f: Boolean
        |  int: Int! @unique
        |  posts: [Post] @relation(link: INLINE)
        |  createdAt: DateTime! @createdAt
        |  updatedAt: DateTime! @updatedAt
        |}
        |
        |type Post {
        |  id: ID! @id
        |  author: User
        |  int: Int! @unique
        |  a: String
        |  b: String
        |  c: String
        |  d: Int
        |  e: Float
        |  f: Boolean
        |  comments: [Comment] @relation(link: INLINE)
        |  createdAt: DateTime! @createdAt
        |  updatedAt: DateTime! @updatedAt
        |}
        |
        |type Comment {
        |  id: ID! @id
        |  int: Int! @unique
        |  a: String
        |  b: String
        |  c: String
        |  d: Int
        |  e: Float
        |  f: Boolean
        |  post: Post
        |  createdAt: DateTime! @createdAt
        |  updatedAt: DateTime! @updatedAt
        |}"""
    }
    database.setup(project)

    var timesFilter     = new ListBuffer[Long]
    var timesFilterDeep = new ListBuffer[Long]
    var findFilter      = new ListBuffer[Long]
    var findFilterDeep  = new ListBuffer[Long]
    var timesFilterOpt  = new ListBuffer[Long]

    val filter     = """query{users(where:{int_gt: 5, int_lt: 19, posts_some:{int_gt: 10000, comments_some: {int_gt:10000}}}){int}}"""
    val filterDeep = """query{users(where:{int_gt: 5,int_lt: 19, posts_some:{int_gt: 10000,comments_some: {int_gt:10000}}}){int, posts{int,comments{int}}}}"""
    val find       = """query{users(where:{int_gt: 5, int_lt: 19}){int}}"""
    val findDeep   = """query{users(where:{int_gt: 5, int_lt: 19}){int, posts{int,comments{int}}}}"""
    val filterOpt  = """query{users(where:{AND:[{posts_some:{int_gt: 10000}}, {posts_some:{int_lt: 10}}, {posts_some:{int: 10}}]}){int}}"""

    val mutStart = System.currentTimeMillis()
    for (x <- 1 to 1000) {
      createData(project, x)
    }
    val mutEnd = System.currentTimeMillis()

    val numQueries = 40

    for (x <- 1 to numQueries) {
      timesFilter += query(project, filter)
    }

    for (x <- 1 to numQueries) {
      timesFilterDeep += query(project, filterDeep)
    }

    for (x <- 1 to numQueries) {
      findFilter += query(project, find)
    }

    for (x <- 1 to numQueries) {
      findFilterDeep += query(project, findDeep)
    }

    for (x <- 1 to numQueries) {
      timesFilterOpt += query(project, filterOpt)
    }

    Thread.sleep(1000)

    println("Data Creation: " + (mutEnd - mutStart))
    println("Filterquery Average: " + (timesFilter.sum / numQueries))
    println("Filterquery Deep Average: " + (timesFilterDeep.sum / numQueries))
    println("Findquery Average: " + (findFilter.sum / numQueries))
    println("Findquery Deep Average: " + (findFilterDeep.sum / numQueries))
    println("FilterOptquery Average: " + (timesFilterOpt.sum / numQueries))
  }

  "Testing with embedded types" should "work" in {
    val project = SchemaDsl.fromStringV11() {
      """
        |type User {
        |  id: ID! @id
        |  a: String
        |  b: String
        |  c: String
        |  d: Int
        |  e: Float
        |  f: Boolean
        |  int: Int! @unique
        |  posts: [Post] @relation(link: INLINE)
        |  createdAt: DateTime! @createdAt
        |  updatedAt: DateTime! @updatedAt
        |}
        |
        |type Post @embedded {
        |  int: Int!
        |  a: String
        |  b: String
        |  c: String
        |  d: Int
        |  e: Float
        |  f: Boolean
        |  comments: [Comment] @relation(link: INLINE)
        |  createdAt: DateTime! @createdAt
        |  updatedAt: DateTime! @updatedAt
        |}
        |
        |type Comment @embedded {
        |  int: Int!
        |  a: String
        |  b: String
        |  c: String
        |  d: Int
        |  e: Float
        |  f: Boolean
        |  createdAt: DateTime! @createdAt
        |  updatedAt: DateTime! @updatedAt
        |}"""
    }
    database.setup(project)

    var timesFilter     = new ListBuffer[Long]
    var timesFilterDeep = new ListBuffer[Long]
    var findFilter      = new ListBuffer[Long]
    var findFilterDeep  = new ListBuffer[Long]

    val filter     = """query{users(where:{int_gt: 5, int_lt: 19, posts_some:{int_gt: 10000, comments_some: {int_gt:10000}}}){int}}"""
    val filterDeep = """query{users(where:{int_gt: 5,int_lt: 19, posts_some:{int_gt: 10000,comments_some: {int_gt:10000}}}){int, posts{int,comments{int}}}}"""
    val find       = """query{users(where:{int_gt: 5, int_lt: 19}){int}}"""
    val findDeep   = """query{users(where:{int_gt: 5, int_lt: 19}){int, posts{int,comments{int}}}}"""

    val mutStart = System.currentTimeMillis()
    for (x <- 1 to 1000) {
      createData(project, x)
    }
    val mutEnd = System.currentTimeMillis()

    val numQueries = 40

    for (x <- 1 to numQueries) {
      timesFilter += query(project, filter)
    }

    for (x <- 1 to numQueries) {
      timesFilterDeep += query(project, filterDeep)
    }

    for (x <- 1 to numQueries) {
      findFilter += query(project, find)
    }

    for (x <- 1 to numQueries) {
      findFilterDeep += query(project, findDeep)
    }

    Thread.sleep(1000)

    println("Data Creation: " + (mutEnd - mutStart))
    println("Filterquery Average: " + (timesFilter.sum / numQueries))
    println("Filterquery Deep Average: " + (timesFilterDeep.sum / numQueries))
    println("Findquery Average: " + (findFilter.sum / numQueries))
    println("Findquery Deep Average: " + (findFilterDeep.sum / numQueries))
  }

  def query(project: Project, query: String): Long = {
    val qStart = System.currentTimeMillis()
    server.query(query, project)
    val qEnd = System.currentTimeMillis()
    qEnd - qStart
  }

  def createData(project: Project, int: Int) = {
    val query = s"""
                   |mutation {
                   |  createUser(data: {
                   |                    int:$int
                   |                    a: "Just a Dummy"
                   |                    b: "Just a Dummy"
                   |                    c: "Just a Dummy"
                   |                    d: 500
                   |                    e: 100.343
                   |                    f: true
                   |                    posts:{create:[
                   |                      {
                   |                        int: ${1000 + int}0
                   |                        a: "Just a Dummy"
                   |                        b: "Just a Dummy"
                   |                        c: "Just a Dummy"
                   |                        d: 500
                   |                        e: 100.343
                   |                        f: true
                   |                        comments:{create:[
                   |                            {int: ${1000 + int}00, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}01, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}02, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}03, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}04, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}05, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}06, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}07, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}08, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}09, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |
                   |                        ]}
                   |                      },
                   |                      {
                   |                        int: ${1000 + int}1
                   |                        a: "Just a Dummy"
                   |                        b: "Just a Dummy"
                   |                        c: "Just a Dummy"     
                   |                        d: 500     
                   |                        e: 100.343
                   |                        f: true
                   |                        comments:{create:[
                   |                            {int: ${1000 + int}10, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}11, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}12, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}13, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}14, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}15, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}16, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}17, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}18, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}19, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                        ]}
                   |                      },
                   |                      {
                   |                        int: ${1000 + int}2
                   |                        a: "Just a Dummy"
                   |                        b: "Just a Dummy"
                   |                        c: "Just a Dummy"
                   |                        d: 500
                   |                        e: 100.343
                   |                        f: true
                   |                        comments:{create:[
                   |                            {int: ${1000 + int}20, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}21, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}22, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}23, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}24, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}25, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}26, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}27, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}28, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                            {int: ${1000 + int}29, a: "Just a Dummy", b: "Just a Dummy", c: "Just a Dummy", d: 5, e: 5.3, f: false}
                   |                        ]}
                   |                      }
                   |                    ]}
    }) {int}} """

    server.query(query, project)
  }
}
