import { Output, Client, Config, getPing } from 'prisma-cli-engine'
import * as inquirer from 'inquirer'
import chalk from 'chalk'
import { Cluster, Environment, PrismaDefinitionClass } from 'prisma-yml'
import {
  concatName,
  defaultDataModel,
  defaultDockerCompose,
  prettyTime,
} from '../util'
import * as sillyname from 'sillyname'
import * as path from 'path'
import * as fs from 'fs'
import { Introspector, PostgresConnector, MysqlConnector } from 'prisma-db-introspection'
import * as yaml from 'js-yaml'
import { Client as PGClient } from 'pg'

export interface GetEndpointParams {
  folderName: string
}

export type DatabaseType = 'postgres' | 'mysql'

export interface DatabaseCredentials {
  type: DatabaseType
  host: string
  port: number
  user: string
  password: string
  database?: string
  alreadyData?: boolean
  schema?: string
  ssl?: boolean
}

export interface GetEndpointResult {
  endpoint: string
  cluster: Cluster | undefined
  workspace: string | undefined
  service: string
  stage: string
  localClusterRunning: boolean
  database?: DatabaseCredentials
  dockerComposeYml: string
  datamodel: string
  newDatabase: boolean
  managementSecret?: string
  writeDockerComposeYml: boolean
  generator?: string
}

export interface HandleChoiceInput {
  choice: string
  loggedIn: boolean
  folderName: string
  localClusterRunning: boolean
  clusters?: Cluster[]
}

const encodeMap = {
  'prisma-eu1': 'demo-eu1',
  'prisma-us1': 'demo-us1',
}

const decodeMap = {
  'demo-eu1': 'prisma-eu1',
  'demo-us1': 'prisma-us1',
}

const defaultPorts = {
  postgres: 5432,
  mysql: 3306,
}

const databaseServiceDefinitions = {
  postgres: `
  postgres:
    image: postgres
    restart: always
    environment:
      POSTGRES_USER: prisma
      POSTGRES_PASSWORD: prisma
    volumes:
      - postgres:/var/lib/postgresql/data
volumes:
  postgres:
`,
  mysql: `
  mysql:
    image: mysql:5.7
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: prisma
    volumes:
      - mysql:/var/lib/mysql
volumes:
  mysql:
`,
}

export interface ConstructorArgs {
  out: Output
  client: Client
  env: Environment
  config: Config
  definition: PrismaDefinitionClass
  shouldAskForGenerator: boolean
}

export class EndpointDialog {
  out: Output
  client: Client
  env: Environment
  config: Config
  definition: PrismaDefinitionClass
  shouldAskForGenerator: boolean
  constructor({
    out,
    client,
    env,
    config,
    definition,
    shouldAskForGenerator,
  }: ConstructorArgs) {
    this.out = out
    this.client = client
    this.env = env
    this.config = config
    this.definition = definition
    this.shouldAskForGenerator = shouldAskForGenerator
  }

  async getEndpoint(): Promise<GetEndpointResult> {
    const localClusterRunning = await this.isClusterOnline(
      'http://localhost:4466',
    )
    const folderName = path.basename(this.config.definitionDir)
    const loggedIn = await this.client.isAuthenticated()
    const clusters = this.getCloudClusters()
    const files = this.listFiles()
    const hasDockerComposeYml = files.includes('docker-compose.yml')
    const question = this.getClusterQuestion(
      !loggedIn && !localClusterRunning,
      hasDockerComposeYml,
      clusters,
    )

    const { choice } = await this.out.prompt(question)

    return this.handleChoice({
      choice: this.decodeName(choice),
      loggedIn,
      folderName,
      localClusterRunning,
      clusters,
    })
  }

  encodeName(name) {
    return encodeMap[name] || name
  }

  decodeName(name) {
    let replaced = name
    Object.keys(decodeMap).forEach(item => {
      if (replaced.includes(item)) {
        replaced = replaced.replace(item, decodeMap[item])
      }
    })
    return replaced
  }

  printDatabaseConfig(credentials: DatabaseCredentials) {
    const defaultDB = JSON.parse(
      JSON.stringify({
        connector: credentials.type,
        host: credentials.host,
        port: credentials.port || defaultPorts[credentials.type],
        database:
          credentials.database && credentials.database.length > 0
            ? credentials.database
            : undefined,
        schema:
          credentials.schema && credentials.schema.length > 0
            ? credentials.schema
            : undefined,
        user: credentials.user,
        password: credentials.password,
        migrations: !credentials.alreadyData,
        rawAccess: true,
      }),
    )
    return yaml
      .safeDump({
        databases: {
          default: defaultDB,
        },
      })
      .split('\n')
      .filter(l => l.trim().length > 0)
      .map(l => `        ${l}`)
      .join('\n')
  }

  printDatabaseService(type: DatabaseType) {
    return databaseServiceDefinitions[type]
  }

  async handleChoice({
    choice,
    loggedIn,
    folderName,
    localClusterRunning,
    clusters = this.getCloudClusters(),
  }: HandleChoiceInput): Promise<GetEndpointResult> {
    let clusterEndpoint
    let cluster: Cluster | undefined
    let workspace: string | undefined
    let service = 'default'
    let stage = 'default'
    let credentials: DatabaseCredentials | undefined
    let dockerComposeYml = defaultDockerCompose
    let datamodel = defaultDataModel
    let newDatabase = false
    let managementSecret: string | undefined
    let writeDockerComposeYml = true

    switch (choice) {
      case 'Use other server':
        clusterEndpoint = await this.customEndpointSelector()
        cluster = new Cluster(this.out, 'custom', clusterEndpoint)
        const needsAuth = await cluster.needsAuth()
        if (needsAuth) {
          managementSecret = await this.ask({
            message: 'Enter the management API secret',
            key: 'managementSecret',
            inputType: 'password',
          })
        }

        service = await this.ask({
          message: 'Choose a name for your service',
          key: 'serviceName',
          defaultValue: folderName,
        })

        stage = await this.ask({
          message: 'Choose a name for your stage',
          key: 'stageName',
          defaultValue: 'dev',
        })

        writeDockerComposeYml = false

        break
      case 'local':
      case 'Create new database':
        cluster =
          (this.env.clusters || []).find(c => c.name === 'local') ||
          new Cluster(this.out, 'local', 'http://localhost:4466')

        const type =
          choice === 'Create new database'
            ? await this.askForDatabaseType()
            : 'mysql'
        credentials = {
          user: type === 'mysql' ? 'root' : 'prisma',
          password: 'prisma',
          type,
          host: type === 'mysql' ? 'mysql' : 'postgres',
          port: defaultPorts[type],
        }
        dockerComposeYml += this.printDatabaseConfig(credentials)
        dockerComposeYml += this.printDatabaseService(type)
        newDatabase = true
        break
      case 'Use existing database':
        credentials = await this.getDatabase()
        this.out.log('')
        const before = Date.now()
        this.out.action.start(
          credentials!.alreadyData
            ? `Introspecting database`
            : `Connecting to database`,
        )
        let connector
        if(credentials.type == 'postgres'){
            const client = new PGClient(this.replaceLocalDockerHost(credentials))
            connector = new PostgresConnector(client)
          } else if(credentials.type == 'mysql'){
            connector = new MysqlConnector(credentials)
          
        }
        const introspector = new Introspector(connector)
        let schemas
        try {
          schemas = await introspector.listSchemas()
        } catch (e) {
          throw new Error(`Could not connect to database. ${e.message}`)
        }

        if (
          credentials &&
          credentials.alreadyData &&
          schemas &&
          schemas.length > 0
        ) {
          const schema = credentials.schema || schemas[0]

          const { numTables, sdl } = await introspector.introspect(schema)
          if(credentials.type === 'postgres'){
            //await this.client.end()
          }
          if (numTables === 0) {
            this.out.log(
              chalk.red(
                `\n${chalk.bold(
                  'Error: ',
                )}The provided database doesn't contain any tables. Please either provide another database or choose "No" for "Does your database contain existing data?"`,
              ),
            )
            this.out.exit(1)
          }

          this.out.action.stop(prettyTime(Date.now() - before))
          this.out.log(
            `Created datamodel definition based on ${numTables} database tables.`,
          )
          datamodel = sdl
        } else {
          this.out.action.stop(prettyTime(Date.now() - before))
        }
        dockerComposeYml += this.printDatabaseConfig(credentials)
        cluster = new Cluster(this.out, 'custom', 'http://localhost:4466')
        break
      case 'Demo server':
        const demoCluster = await this.getDemoCluster()
        if (!demoCluster) {
          return this.getEndpoint()
        } else {
          cluster = demoCluster
        }
        break
      default:
        const result = this.getClusterAndWorkspaceFromChoice(choice)
        if (!result.workspace) {
          cluster = clusters.find(c => c.name === result.cluster)
          if (!loggedIn && cluster && cluster.shared) {
            workspace = this.getPublicName()
          }
        } else {
          cluster = clusters.find(
            c =>
              c.name === result.cluster && c.workspaceSlug === result.workspace,
          )
          workspace = result.workspace
        }
    }

    if (!cluster) {
      throw new Error(`Oops. Could not get cluster.`)
    }

    this.env.setActiveCluster(cluster!)

    // TODO propose alternatives if folderName already taken to ensure global uniqueness
    if (
      !cluster.local ||
      (await this.projectExists(cluster, service, stage, workspace))
    ) {
      service = await this.askForService(folderName)
    }

    if (
      !cluster.local ||
      (await this.projectExists(cluster, service, stage, workspace))
    ) {
      stage = await this.askForStage('dev')
    }

    const generator = this.shouldAskForGenerator
      ? await this.askForGenerator()
      : undefined

    workspace = workspace || cluster.workspaceSlug

    return {
      endpoint: cluster.getApiEndpoint(service, stage, workspace),
      cluster,
      workspace,
      service,
      stage,
      localClusterRunning,
      database: credentials,
      dockerComposeYml,
      datamodel,
      newDatabase,
      managementSecret,
      generator,
      writeDockerComposeYml,
    }
  }

  replaceLocalDockerHost(credentials: DatabaseCredentials) {
    const replaceMap = {
      'host.docker.internal': 'localhost',
      'docker.for.mac.localhost': 'localhost',
    }
    return {
      ...credentials,
      host: replaceMap[credentials.host] || credentials.host,
    }
  }

  async getDatabase(
    introspection: boolean = false,
  ): Promise<DatabaseCredentials> {
    const type = await this.askForDatabaseType(introspection)
    const alreadyData = introspection || (await this.askForExistingData())
    const askForSchema = introspection ? true : alreadyData ? true : false
    const host = await this.ask({
      message: 'Enter database host',
      key: 'host',
      defaultValue: 'localhost',
    })
    const port = await this.ask({
      message: 'Enter database port',
      key: 'port',
      defaultValue: String(defaultPorts[type]),
    })
    const user = await this.ask({
      message: 'Enter database user',
      key: 'user',
    })
    const password = await this.ask({
      message: 'Enter database password',
      key: 'password',
    })
    const database = await this.ask({
      message: alreadyData
        ? `Enter name of existing database`
        : `Enter database name`,
      key: 'database',
    })
        
    const ssl = await this.ask({
      message: 'Use SSL?',
      inputType: 'confirm',
      key: 'ssl',
    })
       
    const schema = 
      type === 'postgres' && askForSchema
      ? await this.ask({
          message: `Enter name of existing schema`,
          key: 'schema',
        })
      : undefined

    return {
      type,
      host,
      port,
      user,
      password,
      database,
      alreadyData,
      schema,
      ssl,
    }
  }

  public async selectSchema(schemas: string[]): Promise<string> {
    const choices = schemas.map(s => ({
      value: s,
      name: s,
    }))

    const { choice } = await this.out.prompt({
      message: 'Please select the Postgres schema you want to introspect',
      name: 'choice',
      type: 'list',
      choices,
      pageSize: Math.min(choices.length, 20),
    })

    return choice
  }

  private getClusterAndWorkspaceFromChoice(
    choice: string,
  ): { workspace: string | null; cluster: string } {
    const splitted = choice.split('/')
    const workspace = splitted.length > 1 ? splitted[0] : null
    const cluster = splitted.slice(-1)[0]

    return { workspace, cluster }
  }

  private getCloudClusters(): Cluster[] {
    if (!this.env.clusters) {
      return []
    }
    return this.env.clusters.filter(c => c.shared || c.isPrivate)
  }

  private async projectExists(
    cluster: Cluster,
    name: string,
    stage: string,
    workspace: string | undefined,
  ): Promise<boolean> {
    try {
      return Boolean(
        await this.client.getProject(
          concatName(cluster, name, workspace || null),
          stage,
        ),
      )
    } catch (e) {
      return false
    }
  }

  private listFiles() {
    return fs.readdirSync(this.config.definitionDir)
  }

  private async isClusterOnline(endpoint: string): Promise<boolean> {
    const cluster = new Cluster(this.out, 'local', endpoint, undefined, true)
    return cluster.isOnline()
  }

  private getClusterQuestion(
    fromScratch: boolean,
    hasDockerComposeYml: boolean,
    clusters: Cluster[],
  ) {
    const sandboxChoices = [
      [
        'Demo server',
        'Hosted demo environment incl. database (requires login)',
      ],
      [
        'Use other server',
        'Manually provide endpoint of a running Prisma server',
      ],
    ]
    if (fromScratch && !hasDockerComposeYml) {
      const fixChoices = [
        ['Use existing database', 'Connect to existing database'],
        ['Create new database', 'Set up a local database using Docker'],
      ]
      const rawChoices = [...fixChoices, ...sandboxChoices]
      const choices = this.convertChoices(rawChoices)
      const finalChoices = [
        new inquirer.Separator('                       '),
        new inquirer.Separator(
          chalk.bold(
            'You can set up Prisma for local development (based on docker-compose)',
          ),
        ),
        ...choices.slice(0, fixChoices.length),
        new inquirer.Separator('                       '),
        new inquirer.Separator(
          chalk.bold('Or deploy to an existing Prisma server:'),
        ),
        ...choices.slice(fixChoices.length, 5),
      ]
      return {
        name: 'choice',
        type: 'list',
        // message: `Connect to your database, set up a new one or use hosted sandbox?`,
        message: `Set up a new Prisma server or deploy to an existing server?`,
        choices: finalChoices,
        pageSize: finalChoices.length,
      }
    } else {
      const clusterChoices =
        clusters.length > 0
          ? clusters.filter(c => !c.shared).map(this.getClusterChoice)
          : sandboxChoices
      const rawChoices = [
        ['Use existing database', 'Connect to existing database'],
        ['Create new database', 'Set up a local database using Docker'],
        ...clusterChoices,
        [
          'Demo server',
          'Hosted demo environment incl. database (requires login)',
        ],
        [
          'Use other server',
          'Manually provide endpoint of a running Prisma server',
        ],
      ]
      const choices = this.convertChoices(rawChoices)
      const dockerChoices = hasDockerComposeYml
        ? []
        : [
            new inquirer.Separator(
              chalk.bold(
                'Set up a new Prisma server for local development (based on docker-compose):',
              ),
            ),
            ...choices.slice(0, 2),
          ]
      const finalChoices = [
        new inquirer.Separator('                       '),
        ...dockerChoices,
        new inquirer.Separator('                       '),
        new inquirer.Separator(
          chalk.bold('Or deploy to an existing Prisma server:'),
        ),
        ...choices.slice(2),
      ]
      return {
        name: 'choice',
        type: 'list',
        message: `Set up a new Prisma server or deploy to an existing server?`,
        choices: finalChoices,
        pageSize: finalChoices.length,
      }
    }
  }

  private getClusterName(c: Cluster): string {
    return `${c.workspaceSlug ? `${c.workspaceSlug}/` : ''}${this.encodeName(
      c.name,
    )}`
  }

  private getClusterChoice = (c: Cluster): string[] => {
    return [this.getClusterName(c), this.getClusterDescription(c)]
  }

  private async getDemoCluster(): Promise<Cluster | null> {
    const isAuthenticated = await this.client.isAuthenticated()
    if (!isAuthenticated) {
      await this.client.login()
    }
    return this.askForDemoCluster()
  }

  private async askForDemoCluster(): Promise<Cluster> {
    const clusters = this.getCloudClusters().slice(0, 2)
    const eu1Cluster = clusters.find(c => c.name === 'prisma-eu1')!
    const us1Cluster = clusters.find(c => c.name === 'prisma-us1')!
    const eu1Ping = await getPing('EU_WEST_1')
    const us1Ping = await getPing('US_WEST_2')
    const eu1Name = this.getClusterName(eu1Cluster)
    const us1Name = this.getClusterName(us1Cluster)
    const eu1Choice = [
      eu1Name,
      `Hosted on AWS in eu-west-1 using MySQL [${eu1Ping.toFixed()}ms latency]`,
    ]
    const us1Choice = [
      us1Name,
      `Hosted on AWS in us-west-2 using MySQL [${us1Ping.toFixed()}ms latency]`,
    ]
    const rawChoices =
      eu1Ping < us1Ping ? [eu1Choice, us1Choice] : [us1Choice, eu1Choice]
    const choices = this.convertChoices(rawChoices)

    const { cluster } = await this.out.prompt({
      name: 'cluster',
      type: 'list',
      message: `Choose the region of your demo server`,
      choices,
    })

    return eu1Name === cluster ? eu1Cluster : us1Cluster
  }

  private getClusterDescription(c: Cluster) {
    if (c.shared) {
      return 'Free development server on Prisma Cloud (incl. database)'
    }

    return `Production Prisma cluster`
  }

  private async askForDatabaseType(introspect: boolean = false) {
    const choices: any[] = []


    choices.push({
      value: 'mysql',
      name:
        'MySQL             MySQL compliant databases like MySQL or MariaDB',
      short: 'MySQL',
    })
    

    choices.push({
      value: 'postgres',
      name: 'PostgreSQL        PostgreSQL database',
      short: 'PostgreSQL',
    })

    const { dbType } = await this.out.prompt({
      name: 'dbType',
      type: 'list',
      message: `What kind of database do you want to ${
        introspect ? 'introspect' : 'deploy to'
      }?`,
      choices,
      // pageSize: 9,
    })

    return dbType
  }

  private convertChoices(
    choices: string[][],
  ): Array<{ value: string; name: string }> {
    const padded = this.out.printPadded(choices, 0, 6).split('\n')
    return padded.map((name, index) => ({
      name,
      value: choices[index][0],
      short: choices[index][0],
    }))
  }

  private async askForStage(defaultName: string): Promise<string> {
    const question = {
      name: 'stage',
      type: 'input',
      message: 'Choose a name for your stage',
      default: defaultName,
    }

    const { stage } = await this.out.prompt(question)

    return stage
  }

  private async askForGenerator(): Promise<string> {
    const choices = [
      {
        name: 'Prisma TypeScript Client',
        value: 'typescript-client',
      },
      {
        name: 'Prisma Flow Client',
        value: 'flow-client',
      },
      {
        name: 'Prisma JavaScript Client',
        value: 'javascript-client',
      },
      {
        name: 'Prisma Go Client',
        value: 'go-client',
      },
    ]

    const { generator } = await this.out.prompt({
      name: 'generator',
      type: 'list',
      message:
        'Select the programming language for the generated Prisma client',
      pageSize: choices.length,
      choices,
    })

    return generator
  }

  private async askForService(defaultName: string): Promise<string> {
    const question = {
      name: 'service',
      type: 'input',
      message: 'Choose a name for your service',
      default: defaultName,
    }

    const { service } = await this.out.prompt(question)

    return service
  }

  private async customEndpointSelector(): Promise<string> {
    const question = {
      name: 'endpoint',
      type: 'input',
      message: `Enter the endpoint of your Prisma server`,
    }

    const { endpoint } = await this.out.prompt(question)

    return endpoint
  }

  private async askForExistingData(): Promise<boolean> {
    const question = {
      name: 'existingData',
      type: 'list',
      message: `Does your database contain existing data?`,
      choices: [
        {
          value: 'no',
          name: 'No',
        },
        {
          value: 'yes',
          name: 'Yes (experimental - Prisma migrations not yet supported)',
          short: 'Yes',
        },
        new inquirer.Separator(
          `\n\n${chalk.yellow(
            'Warning: Introspecting databases with existing data is currently an experimental feature. If you find any issues, please report them here: https://github.com/prisma/prisma/issues\n',
          )}`,
        ),
      ],
      pageSize: 10,
    }

    const { existingData } = await this.out.prompt(question)
    return existingData === 'yes'
  }

  private async ask({
    message,
    defaultValue,
    key,
    validate,
    required,
    inputType = 'input',
  }: {
    message: string
    key: string
    defaultValue?: string
    validate?: (value: string) => boolean | string
    required?: boolean
    inputType?: string
  }) {
    const question = {
      name: key,
      type: inputType,
      message,
      default: defaultValue,
      validate:
        defaultValue || !required
          ? undefined
          : validate ||
            (value =>
              value && value.length > 0
                ? true
                : `Please provide a valid ${key}`),
    }

    const result = await this.out.prompt(question)

    return result[key]
  }

  private getSillyName() {
    return `${slugify(sillyname()).split('-')[0]}-${Math.round(
      Math.random() * 1000,
    )}`
  }

  private getPublicName() {
    return `public-${this.getSillyName()}`
  }
}

function slugify(text) {
  return text
    .toString()
    .toLowerCase()
    .replace(/\s+/g, '-') // Replace spaces with -
    .replace(/[^\w\-]+/g, '') // Remove all non-word chars
    .replace(/\-\-+/g, '-') // Replace multiple - with single -
    .replace(/^-+/, '') // Trim - from start of text
    .replace(/-+$/, '') // Trim - from end of text
}
