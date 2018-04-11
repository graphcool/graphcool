package com.prisma.deploy.connector.mysql.database

import slick.dbio.DBIOAction
import slick.dbio.Effect.Read
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.meta.{DatabaseMeta, MTable}
import scala.concurrent.ExecutionContext.Implicits.global

object IntrospectionQueryBuilder {
  case class ColumnDescription(name: String, isNullable: Boolean, typeName: String, size: Option[Int])
  case class IndexDescription(name: Option[String], nonUnique: Boolean, column: Option[String])
  case class ForeignKeyDescription(name: Option[String], column: String, foreignTable: String, foreignColumn: String)
  case class TableInfo(name: String, columns: List[ColumnDescription], indexes: List[IndexDescription], foreignKeys: List[ForeignKeyDescription])

  def getTableInfo(schema: String, tableName: String): DBIOAction[TableInfo, NoStream, Read] = {
    for {
      metaTables <- MTable
                     .getTables(cat = Some(schema), schemaPattern = None, namePattern = Some(tableName), types = None)
      columns     <- metaTables.head.getColumns
      indexes     <- metaTables.head.getIndexInfo(false, false)
      foreignKeys <- metaTables.head.getImportedKeys
    } yield
      TableInfo(
        name = tableName,
        columns = columns
          .map(x => ColumnDescription(name = x.name, isNullable = x.isNullable.get, typeName = x.typeName, size = x.size))
          .toList,
        indexes = indexes
          .map(x => IndexDescription(name = x.indexName, nonUnique = x.nonUnique, column = x.column))
          .toList,
        foreignKeys = foreignKeys
          .map(x => ForeignKeyDescription(name = x.fkName, column = x.fkColumn, foreignColumn = x.pkColumn, foreignTable = x.pkTable.name))
          .toList
      )
  }

  def getTables(schema: String) = {
    for {
      metaTables <- MTable.getTables(cat = Some(schema), schemaPattern = None, namePattern = None, types = None)
    } yield metaTables.map(table => table.name.name)
  }

  def getSchemas: DBIOAction[Vector[String], NoStream, Read] = {
    for {
      catalogs <- DatabaseMeta.getCatalogs
    } yield catalogs
  }
}
