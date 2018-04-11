package com.prisma.api.connector.mysql

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.prisma.api.ApiDependenciesForTest
import com.prisma.deploy.specutils.DeployDependenciesForTest
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.MySQLProfile.backend.DatabaseDef

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class mysqlDatabaseIntrospectorSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  implicit lazy val system                     = ActorSystem()
  implicit lazy val materializer               = ActorMaterializer()
  implicit lazy val apiDependencies            = new ApiDependenciesForTest
  implicit lazy val deployDependencies         = new DeployDependenciesForTest()
  private lazy val clientDatabase: DatabaseDef = apiDependencies.apiConnector.asInstanceOf[MySqlApiConnectorImpl].databases.master

  val DBIntrospector = deployDependencies.deployPersistencePlugin.databaseIntrospector

  override def beforeEach() {
    val query =
      DBIO.seq(
        sqlu"""DROP SCHEMA IF EXISTS `DatabaseIntrospector`;""",
        sqlu"""CREATE SCHEMA `DatabaseIntrospector` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; """
      )
    Await.result(clientDatabase.run(query), Duration.Inf)
  }

  def run(query: slick.dbio.DBIOAction[_, slick.dbio.NoStream, Effect.All]) = {

    Await.result(clientDatabase.run(
                   DBIO.seq(
                     sqlu"""USE DatabaseIntrospector;""",
                     query
                   )),
                 Duration.Inf)
  }

  "DatabaseIntrospector" should "list schemas" in {
    val collections = Await.result(DBIntrospector.listCollections, Duration.Inf)

    collections should contain("DatabaseIntrospector")
  }

  "String columns" should "be generated correctly" in {

    run(sqlu"""
        CREATE TABLE "Strings" (
          "a" char(1) DEFAULT NULL,
          "b" varchar(255) DEFAULT NULL,
          "c" tinytext,
          "d" text,
          "e" mediumtext,
          "f" longtext,
          "g" char(1) NOT NULL,
          "h" varchar(255) NOT NULL,
          "i" tinytext NOT NULL,
          "j" text NOT NULL,
          "k" mediumtext NOT NULL,
          "l" longtext NOT NULL
        );
      """)

    val sdl = Await.result(DBIntrospector.generateSchema("DatabaseIntrospector"), Duration.Inf)

    sdl should be("""type Strings {
                    |  a: String
                    |  b: String
                    |  c: String
                    |  d: String
                    |  e: String
                    |  f: String
                    |  g: String!
                    |  h: String!
                    |  i: String!
                    |  j: String!
                    |  k: String!
                    |  l: String!
                    |}""".stripMargin)
  }

  "Int columns" should "be generated correctly" in {

    run(sqlu"""
        CREATE TABLE "Ints" (
          "a" tinyint DEFAULT NULL,
          "b" smallint DEFAULT NULL,
          "c" mediumint,
          "d" int,
          "e" bigint,
          "f" year,
          "g" tinyint NOT NULL,
          "h" smallint NOT NULL,
          "i" mediumint NOT NULL,
          "j" int NOT NULL,
          "k" year NOT NULL
        );
      """)

    val sdl = Await.result(DBIntrospector.generateSchema("DatabaseIntrospector"), Duration.Inf)

    sdl should be("""type Ints {
                    |  a: Int # This might be a Boolean
                    |  b: Int
                    |  c: Int
                    |  d: Int
                    |  e: Int
                    |  f: Int
                    |  g: Int! # This might be a Boolean
                    |  h: Int!
                    |  i: Int!
                    |  j: Int!
                    |  k: Int!
                    |}""".stripMargin)
  }

  "Float columns" should "be generated correctly" in {

    run(sqlu"""
        CREATE TABLE "Floats" (
          "a" float DEFAULT NULL,
          "b" double DEFAULT NULL,
          "c" float NOT NULL,
          "d" double NOT NULL
        );
      """)

    val sdl = Await.result(DBIntrospector.generateSchema("DatabaseIntrospector"), Duration.Inf)

    sdl should be("""type Floats {
                    |  a: Float
                    |  b: Float
                    |  c: Float!
                    |  d: Float!
                    |}""".stripMargin)
  }

  "Boolean columns" should "be generated correctly" in {

    run(sqlu"""
        CREATE TABLE "Booleans" (
          "a" bool DEFAULT NULL,
          "b" bit DEFAULT NULL,
          "c" bool NOT NULL,
          "d" bit NOT NULL
        );
      """)

    val sdl = Await.result(DBIntrospector.generateSchema("DatabaseIntrospector"), Duration.Inf)

    sdl should be("""type Booleans {
                    |  a: Boolean
                    |  b: Boolean
                    |  c: Boolean!
                    |  d: Boolean!
                    |}""".stripMargin)
  }

  "DateTime columns" should "be generated correctly" in {

    run(sqlu"""
        CREATE TABLE "DateTimes" (
          "a" datetime DEFAULT NULL,
          "b" timestamp DEFAULT NULL,
          "c" datetime NOT NULL,
          "d" timestamp NOT NULL
        );
      """)

    val sdl = Await.result(DBIntrospector.generateSchema("DatabaseIntrospector"), Duration.Inf)

    sdl should be("""type DateTimes {
                    |  a: DateTime
                    |  b: DateTime
                    |  c: DateTime!
                    |  d: DateTime!
                    |}""".stripMargin)
  }
}
