import {
  Connector,
  Table,
  Column,
  TypeIdentifier,
  TableRelation,
  PrimaryKey,
  MysqlConnectionDetails,
} from '../types/common'
import * as _ from 'lodash'
import { Client } from 'pg'
import { createConnection, Connection, Query, ConnectionOptions } from 'mysql2';

// Responsible for extracting a normalized representation of a PostgreSQL database (schema)
export class MysqlConnector implements Connector {
  connectionPromise: Connection
  connectionOptions: ConnectionOptions
  mysql = require('mysql2/promise');

  constructor(connectionDetails: MysqlConnectionDetails) {
    this.connectionOptions = connectionDetails
  }

  async listSchemas(): Promise<string[]> {
    return await this.querySchemas()
  }

  async listTables(schemaName: string): Promise<Table[]> {
    const [relations, tableColumns, primaryKeys] = await Promise.all([
      this.queryRelations(schemaName),
      this.queryTableColumns(schemaName),
      this.queryPrimaryKeys(schemaName)
    ])

    const tables = _.map(tableColumns, (rawColumns, tableName) => {
      const tablePrimaryKey = primaryKeys.find(pk => pk.tableName === tableName) || null
      const tableRelations = relations.filter(rel => rel.source_table === tableName)
      const columns = _.map(rawColumns, column => {
        // Ignore composite keys for now
        const isPk = Boolean(tablePrimaryKey
          && tablePrimaryKey.fields.length == 1
          && Boolean(tablePrimaryKey.fields.includes(column.COLUMN_NAME))
        )

        const { typeIdentifier, comment, error } = this.toTypeIdentifier(
          column.DATA_TYPE,
          column.COLUMN_NAME,
          isPk
        )

        const col = {
          isUnique: column.is_unique || isPk,
          isPrimaryKey: isPk,
          defaultValue: this.parseDefaultValue(column),
          name: column.COLUMN_NAME,
          type: column.DATA_TYPE,
          typeIdentifier,
          comment,
          nullable: column.IS_NULLIBLE === 'YES',
        } as Column

        return col
      }).filter(x => x != null) as Column[]
      return new Table(tableName, columns, relations)
    })

    return _.sortBy(tables, x => x.name)
  }

  // Queries all columns of all tables in given schema and returns them grouped by table_name
  async queryTableColumns(schemaName: string): Promise<{ [key: string]: any[] }> {
    const res = await this.connectionPromise.execute(
      `SELECT *, (SELECT EXISTS(
        SELECT *
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE constraint_type = 'UNIQUE' 
        AND tc.constraint_schema = ?
        AND tc.table_name = c.table_name
        AND kcu.column_name = c.column_name)) as is_unique
      FROM  information_schema.columns c
      WHERE table_schema = ?`,
      [schemaName, schemaName]
    )

    return _.groupBy(this.parseResult(res[0]), 'TABLE_NAME')
  }

  /**
   * 
   * @param str response from mysql query
   * 
   */
  parseResult(str){
    return JSON.parse(JSON.stringify(str))
  }

  async queryPrimaryKeys(schemaName: string): Promise<PrimaryKey[]> {
    const keys = await this.connectionPromise.execute(
      `select table_name, 
      column_name
      from information_schema.columns
      where table_schema = ?
      and column_key = 'PRI'`,
      [schemaName]
    )
    
    const grouped = _.groupBy(this.parseResult(keys[0]), 'table_name')
    return _.map(grouped, (pks, key) => {
      return {
        tableName: key,
        fields: pks.map(x => x.column_name)
      } as PrimaryKey
    })
  }

  async queryRelations(schemaName: string): Promise<TableRelation[]> {
    const res = await this.connectionPromise.execute(
      `SELECT 
      TABLE_NAME,                            
      COLUMN_NAME,            
      REFERENCED_TABLE_NAME,                 
      REFERENCED_COLUMN_NAME                 
    FROM
      INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE
      TABLE_SCHEMA = ?
      AND REFERENCED_TABLE_NAME IS NOT NULL`,
      [schemaName]
    )
   
    return this.parseResult(res[0]).map(row => {
      return {
        source_table: row.TABLE_NAME,
        source_column: row.COLUMN_NAME,
        target_table: row.REFERENCED_TABLE_NAME,
        target_column: row.REFERENCED_COLUMN_NAME
      }
    }) as TableRelation[]
  }

  async querySchemas(){
    this.connectionPromise = await this.mysql.createConnection(this.connectionOptions)
    const res = await this.connectionPromise.execute(
      `select ? as db`, [this.connectionOptions.database]
    )
    
    if(res[0].length){
      return res[0].map(x => x.db)
    }
  }

  parseDefaultValue(column) {
    
    if (column.column_default == null) {
      return null
    }

    if (column.extra.indexOf(`auto_increment`) != -1) {
      return '[AUTO INCREMENT]'
    }

    if (column.column_default.indexOf('TIMESTAMP') != -1) {
      return null
    }
    return column.column_default
  }

  toTypeIdentifier(
    type: string,
    field: string,
    isPrimaryKey: boolean
  ): {
      typeIdentifier: TypeIdentifier | null
      comment: string | null
      error: string | null
    } {
    if (
      isPrimaryKey &&
      (type === 'varchar')
    ) {
      return { typeIdentifier: 'String', comment: null, error: null }
    }

    if (type === 'tinyint') {
      return { typeIdentifier: 'Boolean', comment: null, error: null }
    }
    
    if (type === 'int' && isPrimaryKey) {
      return { typeIdentifier: 'ID', comment: null, error: null }
    }
    if (type === 'varchar' || type.indexOf(`text`) != -1) {
      return { typeIdentifier: 'String', comment: null, error: null }
    }
    if (type.indexOf(`int`) != -1) {
      return { typeIdentifier: 'Int', comment: null, error: null }
    }
    if (type === 'decimal') {
      return { typeIdentifier: 'Float', comment: null, error: null }
    }
    if (type === 'timestamp') {
      return { typeIdentifier: 'DateTime', comment: null, error: null }
    }
    if (type === 'datetime') {
      return { typeIdentifier: 'DateTime', comment: null, error: null }
    }
    if (type === 'json') {
      return { typeIdentifier: 'Json', comment: null, error: null }
    }
    if (type === 'date') {
      return { typeIdentifier: 'DateTime', comment: null, error: null }
    }

    return {
      typeIdentifier: null,
      comment: `Type '${type}' is not yet supported.`,
      error: `Not able to handle type '${type}'`,
    }
  }
}
