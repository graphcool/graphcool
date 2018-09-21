import { Command, flags, Flags } from 'prisma-cli-engine'
import { EndpointDialog } from '../../utils/EndpointDialog'

import {
  Introspector,
  PostgresConnector,
  MysqlConnector,
  PrismaDBClient,
} from 'prisma-db-introspection'

import * as path from 'path'
import * as fs from 'fs'
import { prettyTime } from '../../util'
import chalk from 'chalk'
import { Client } from 'pg'

export default class IntrospectCommand extends Command {
  static topic = 'introspect'
  static description = 'Introspect database schema(s) of service'
  static flags: Flags = {
    interactive: flags.boolean({
      char: 'i',
      description: 'Interactive mode',
    }),
    ['pg-schema-name']: flags.string({
      description: 'Name of the Postgres schema',
      char: 'p',
    }),
  }
  static hidden = false
  async run() {
    const { interactive } = this.flags
    const pgSchemaName = this.flags['pg-schema-name']

    const endpointDialog = new EndpointDialog({
      out: this.out,
      client: this.client,
      env: this.env,
      config: this.config,
      definition: this.definition,
      shouldAskForGenerator: false,
    })

    let client

    if (interactive) {
      const credentials = await endpointDialog.getDatabase(true)
      client = new Client(credentials)
    } else {
      await this.definition.load(this.flags)
      client = new PrismaDBClient(this.definition)
    }

    const credentials = await endpointDialog.getDatabase(true)
    
    let connector
    if(credentials.type == 'mysql'){
      connector = new MysqlConnector(credentials)
    } else if (credentials.type == 'postgres'){
      const client = new Client(endpointDialog.replaceLocalDockerHost(credentials))
      connector = new PostgresConnector(client)
    }
    
    const introspector = new Introspector(connector)
    let schemas
    const before = Date.now()
    try {
      schemas = await introspector.listSchemas()
      
    } catch (e) {
      throw new Error(`Could not connect to database. ${e.message}`)
    }
    if (schemas && schemas.length > 0) {
      let schema
      if (schemas.length === 1) {
        schema = schemas[0]
      } else if (pgSchemaName) {
        const exists = schemas.includes(pgSchemaName)
        if (!exists) {
          throw new Error(
            `The provided Postgres Schema ${pgSchemaName} does not exist. Choose one of ${schemas.join(
              ', ',
            )}`,
          )
        }
        schema = pgSchemaName
      } else {
        const schemaName = `${this.definition.service}$${this.definition.stage}`
        const exists = schemas.includes(schemaName)
        schema = exists
          ? schemaName
          : await endpointDialog.selectSchema(
              schemas.filter(
                s => !s.startsWith('prisma-temporary-introspection-service$'),
              ),
            )
      }

      this.out.action.start(`Introspecting schema ${chalk.bold(schema)}`)
      const { sdl, numTables } = await introspector.introspect(schema)
      await client.end()
      if (numTables === 0) {
        this.out.log(
          chalk.red(
            `\n${chalk.bold(
              'Error: ',
            )}The provided database doesn't contain any tables. Please provide another database.`,
          ),
        )
        this.out.exit(1)
      }
      const fileName = `datamodel-${new Date().getTime()}.prisma`
      const fullFileName = path.join(this.config.definitionDir, fileName)
      fs.writeFileSync(fullFileName, sdl)
      this.out.action.stop(prettyTime(Date.now() - before))
      this.out.log(
        `Created datamodel definition based on ${numTables} database tables.`,
      )
      this.out.log(`\
${chalk.bold(
        'Created 1 new file:',
      )}    GraphQL SDL-based datamodel (derived from existing database)

  ${chalk.cyan(fileName)}
`)
      if (!this.definition.definition!.datamodel) {
        this.definition.addDatamodel(fileName)
        this.out.log(
          `Added ${chalk.bold(`datamodel: ${fileName}`)} to prisma.yml`,
        )
      }
    } else {
      throw new Error(`Could not find schema in provided database.`)
    }
  }
}
