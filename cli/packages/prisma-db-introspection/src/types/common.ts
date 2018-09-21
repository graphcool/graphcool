import { ClientConfig } from 'pg'

export type TypeIdentifier =
  | 'String'
  | 'Int'
  | 'Float'
  | 'Boolean'
  | 'DateTime'
  | 'ID'
  | 'UUID'
  | 'Json' // | 'Enum' | 'Relation'

export interface Connector {
  listSchemas(): Promise<string[]>
  listTables(schemaName: string): Promise<Table[]>
}

export class Table {
  name: string
  columns: Column[]
  relations: TableRelation[]

  constructor(name: string, columns: Column[], relations: TableRelation[]) {
    this.name = name
    this.columns = columns
    this.relations = relations
  }

  hasPrimaryKey(): boolean {
    return this.columns.some(x => {
      return x.isPrimaryKey
    })
  }

  isJoinTable(): boolean {
    // Table is a join table, if:
    // - It has 2 relations that are not self-relations
    const condition1 =
      this.relations.filter(rel => rel.target_table !== this.name).length === 2
    // - It has no primary key (Prisma doesn't handle join tables with keys)
    const condition2 = !this.columns.some(c => c.isPrimaryKey)
    // - It has only other fields that are nullable or have default values (Prisma doesn't set other fields on join tables)
    const condition3 = !this.columns
      .filter(c => !this.isRelationColumn(c) !== null)
      .some(c => !c.nullable && c.defaultValue === null)
    return condition1 && condition2 && condition3
  }

  isRelationColumn(column: Column): boolean {
    return this.relations.some(rel => rel.source_column == column.name)
  }
}

export interface Column {
  name: string
  isUnique: boolean
  isPrimaryKey: boolean
  defaultValue: any
  type: string
  typeIdentifier: TypeIdentifier
  comment: string | null
  nullable: boolean
}

export interface TableRelation {
  source_table: string
  target_table: string
  source_column: string
  target_column: string
}

export interface PrimaryKey {
  tableName: string
  fields: string[]
}

export type PostgresConnectionDetails = string | ClientConfig

export interface DBClient {
  query(queryText: string, values?: any[]): Promise<QueryResult>
  connect(): Promise<void>
  end(): Promise<void>
}

export interface MysqlConnectionDetails {
  host: string
  port: number
  user: string
  password: string
  database?: string
} 

export interface QueryResult {
  rows: any[]
}
