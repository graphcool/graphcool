import { Output, Client, Config, getPing } from 'prisma-cli-engine'
import * as inquirer from 'inquirer'
import chalk from 'chalk'
import { Cluster, Environment } from 'prisma-yml'
import { concatName, defaultDataModel, defaultDockerCompose } from '../util'
import * as sillyname from 'sillyname'
import * as path from 'path'
import * as fs from 'fs'
import { Introspector } from 'prisma-db-introspection'
import { defaultDBPort } from '../commands/local/constants'
import * as yaml from 'js-yaml'

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
}

export interface HandleChoiceInput {
  choice: string
  loggedIn: boolean
  folderName: string
  localClusterRunning: boolean
  clusters?: Cluster[]
}

const encodeMap = {
  'prisma-eu1': 'sandbox-eu1',
  'prisma-us1': 'sandbox-us1',
}

const decodeMap = {
  'sandbox-eu1': 'prisma-eu1',
  'sandbox-us1': 'prisma-us1',
}

const defaultPorts = {
  postgres: 5432,
  mysql: 3306,
}

const databaseServiceDefinitions = {
  postgres: `
  db:
    image: postgres
    restart: always
    environment:
      POSTGRES_USER: prisma
      POSTGRES_PASSWORD: prisma
`,
  mysql: `
  db:
    image: mysql:5.7
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: prisma
`,
}

export class EndpointDialog {
  out: Output
  client: Client
  env: Environment
  config: Config
  constructor(out: Output, client: Client, env: Environment, config: Config) {
    this.out = out
    this.client = client
    this.env = env
    this.config = config
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
        migrations: !credentials.alreadyData,
        host: credentials.host,
        port: credentials.port || defaultPorts[credentials.type],
        user: credentials.user,
        password: credentials.password,
        database:
          credentials.database && credentials.database.length > 0
            ? credentials.database
            : undefined,
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
    let credentials
    let dockerComposeYml = defaultDockerCompose
    let datamodel = defaultDataModel
    let newDatabase = false

    switch (choice) {
      case 'Use other server':
        clusterEndpoint = await this.customEndpointSelector(folderName)
        cluster = new Cluster(this.out, 'custom', clusterEndpoint)
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
          host: 'db',
          port: defaultPorts[type],
        }
        dockerComposeYml += this.printDatabaseConfig(credentials)
        dockerComposeYml += this.printDatabaseService(type)
        newDatabase = true
        break
      case 'Use existing database':
        credentials = await this.getDatabase()
        this.out.action.start(`Connecting to database`)
        const introspector = new Introspector(
          this.replaceLocalDockerHost(credentials),
        )
        let schemas
        try {
          schemas = await introspector.listSchemas()
        } catch (e) {
          throw new Error(`Could not connect to database. ${e.message}`)
        }
        // TODO: ask for postres schema if more than one
        if (schemas && schemas.length > 0) {
          const { numTables, sdl } = await introspector.introspect(schemas[0])
          if (numTables === 0) {
            throw new Error(
              `The provided database doesn't contain any tables. Please either provide another database or choose "No" for "What kind of database do you want to deploy to?"`,
            )
          }
          datamodel = sdl

          dockerComposeYml += this.printDatabaseConfig(credentials)
        }
        this.out.action.stop()
        cluster = new Cluster(this.out, 'custom', 'http://localhost:4466')
        break
      case 'Demo Server':
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

  async getDatabase(): Promise<DatabaseCredentials> {
    const type = await this.askForDatabaseType()
    const alreadyData = await this.ask({
      message: 'Do you already have data in the database? (yes/no)',
      key: 'alreadyData',
      defaultValue: 'no',
      validate: value =>
        ['yes', 'no'].includes(value) ? true : 'Please answer either yes or no',
    })
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
    const database =
      type === 'postgres'
        ? await this.ask({
            message: 'Enter database name',
            key: 'database',
          })
        : null

    return {
      type,
      host,
      port,
      user,
      password,
      database,
      alreadyData,
    }
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
        'Demo Server',
        'Hosted demo environment incl. database (requires login)',
      ],
      [
        'User other server',
        'Manually provide endpoint of a running Prisma Server',
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
            'You can set up Prisma  for local development (requires Docker)',
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
          ? clusters.map(this.getClusterChoice)
          : sandboxChoices
      const rawChoices = [
        ['local', 'Local Prisma server'],
        ...clusterChoices,
        ['Use other server', 'Connect to an existing prisma server'],
        ['Use existing database', 'Connect to existing database'],
        ['Create new database', 'Set up a local database using Docker'],
      ]
      const choices = this.convertChoices(rawChoices)
      const dockerChoices = hasDockerComposeYml
        ? []
        : [
            new inquirer.Separator(
              chalk.bold(
                'Set up a new Prisma server for local development (requires Docker):',
              ),
            ),
            ...choices.slice(choices.length - 2),
          ]
      const finalChoices = [
        new inquirer.Separator('                       '),
        new inquirer.Separator(chalk.bold('Use an existing Prisma server')),
        ...choices.slice(0, clusterChoices.length + 2),
        new inquirer.Separator('                       '),
        ...dockerChoices,
      ]
      return {
        name: 'choice',
        type: 'list',
        message: `Connect to your database, set up a new one or use existing Prisma server?`,
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
    if (!this.env.cloudSessionKey) {
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
      `Hosted on AWS in us-west-2 using MySQL [${eu1Ping.toFixed()}ms latency]`,
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

  private async askForDatabaseType() {
    const { dbType } = await this.out.prompt({
      name: 'dbType',
      type: 'list',
      message: `What kind of database do you want to deploy to?`,
      choices: [
        {
          value: 'mysql',
          name:
            'MySQL             MySQL compliant databases like MySQL or MariaDB',
        },
        {
          value: 'postgres',
          name: 'PostgreSQL        PostgreSQL database',
        },
      ],
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
    }))
  }

  private async askForStage(defaultName: string): Promise<string> {
    const question = {
      name: 'stage',
      type: 'input',
      message: 'To which stage do you want to deploy?',
      default: defaultName,
    }

    const { stage } = await this.out.prompt(question)

    return stage
  }

  private async askForService(defaultName: string): Promise<string> {
    const question = {
      name: 'service',
      type: 'input',
      message: 'How do you want to call your service?',
      default: defaultName,
    }

    const { service } = await this.out.prompt(question)

    return service
  }

  private async customEndpointSelector(defaultName: string): Promise<string> {
    const question = {
      name: 'endpoint',
      type: 'input',
      message: `What's your clusters endpoint?`,
      default: defaultName,
    }

    const { endpoint } = await this.out.prompt(question)

    return endpoint
  }

  private async ask({
    message,
    defaultValue,
    key,
    validate,
    required,
  }: {
    message: string
    key: string
    defaultValue?: string
    validate?: (value: string) => boolean | string
    required?: boolean
  }) {
    const question = {
      name: key,
      type: 'input',
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
