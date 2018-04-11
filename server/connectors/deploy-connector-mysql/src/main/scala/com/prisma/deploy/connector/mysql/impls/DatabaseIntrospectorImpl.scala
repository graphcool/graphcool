package com.prisma.deploy.connector.mysql.impls

import com.prisma.deploy.connector.DatabaseIntrospector
import com.prisma.deploy.connector.mysql.database.IntrospectionQueryBuilder
import com.prisma.deploy.connector.mysql.database.IntrospectionQueryBuilder.{ColumnDescription, TableInfo}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

case class DatabaseIntrospectorImpl(clientDb: Database)(implicit ec: ExecutionContext) extends DatabaseIntrospector {
  override def listCollections: Future[Vector[String]] = clientDb.run(IntrospectionQueryBuilder.getSchemas)

  override def generateSchema(collection: String): Future[String] = {
    for {
      tables: Seq[String] <- clientDb.run(IntrospectionQueryBuilder.getTables(collection))
      tableInfos          <- Future.sequence(tables.map(table => clientDb.run(IntrospectionQueryBuilder.getTableInfo(collection, table))))
    } yield {
      tableInfos
        .map {
          case table: TableInfo =>
            s"""|type ${table.name} {
                |  ${table.columns.map(printColumn).mkString("", System.getProperty("line.separator") + "  ", "")}
                |}""".stripMargin
        }
        .mkString("", System.getProperty("line.separator") + "  ", "")
    }
  }

  private def printColumn(column: ColumnDescription) = {
    val base = column.typeName.toLowerCase match {
      case "char"       => s"${column.name}: String!"
      case "varchar"    => s"${column.name}: String!"
      case "tinytext"   => s"${column.name}: String!"
      case "text"       => s"${column.name}: String!"
      case "mediumtext" => s"${column.name}: String!"
      case "longtext"   => s"${column.name}: String!"
      case "tinyint"    => s"${column.name}: Int! # This might be a Boolean"
      case "smallint"   => s"${column.name}: Int!"
      case "mediumint"  => s"${column.name}: Int!"
      case "int"        => s"${column.name}: Int!"
      case "bigint"     => s"${column.name}: Int!"
      case "year"       => s"${column.name}: Int!"
      case "float"      => s"${column.name}: Float!"
      case "double"     => s"${column.name}: Float!"
      case "bit"        => s"${column.name}: Boolean!"
      case "datetime"   => s"${column.name}: DateTime!"
      case "timestamp"  => s"${column.name}: DateTime!"
      case x            => s"# type `$x` not supported. Try adding column `${column.name}` manually"
    }

    if (column.isNullable) {
      base.replace("!", "")
    } else {
      base
    }
  }
}
