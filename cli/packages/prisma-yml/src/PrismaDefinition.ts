import { readDefinition } from './yaml'
import { PrismaDefinition } from 'prisma-json-schema'
import * as fs from 'fs-extra'
import * as dotenv from 'dotenv'
import * as dotenvExpand from 'dotenv-expand'
import * as path from 'path'
import * as jwt from 'jsonwebtoken'
import { Args } from './types/common'
import { Environment } from './Environment'
import { IOutput } from './Output'
import { Cluster } from './Cluster'
import { FunctionInput, Header } from './types/rc'
import chalk from 'chalk'
import { replaceYamlValue } from './utils/yamlComment'
import { parseEndpoint } from './utils/parseEndpoint'
const debug = require('debug')('prisma definition')

interface ErrorMessage {
  message: string
}

export interface EnvVars {
  [key: string]: string | undefined
}

export type HookType = 'post-deploy'

export class PrismaDefinitionClass {
  definition?: PrismaDefinition
  rawJson?: any
  typesString?: string
  secrets: string[] | null
  definitionPath?: string | null
  definitionDir: string
  env: Environment
  out?: IOutput
  envVars: any
  rawEndpoint?: string
  private definitionString: string
  constructor(
    env: Environment,
    definitionPath?: string | null,
    envVars: EnvVars = process.env,
    out?: IOutput,
  ) {
    this.secrets = null
    this.definitionPath = definitionPath
    if (definitionPath) {
      this.definitionDir = path.dirname(definitionPath)
    }
    this.env = env
    this.out = out
    this.envVars = envVars
  }
  async load(args: Args, envPath?: string) {
    if (envPath) {
      if (!fs.pathExistsSync(envPath)) {
        envPath = path.join(process.cwd(), envPath)
      }

      if (!fs.pathExistsSync(envPath)) {
        throw new Error(`--env-file path '${envPath}' does not exist`)
      }
    }
    dotenvExpand(dotenv.config({ path: envPath }))
    if (this.definitionPath) {
      await this.loadDefinition(args)

      this.validate()
    } else {
      throw new Error(
        `Couldn’t find \`prisma.yml\` file. Are you in the right directory?`,
      )
    }
  }

  private async loadDefinition(args) {
    const { definition, rawJson } = await readDefinition(
      this.definitionPath!,
      args,
      this.out,
      this.envVars,
    )
    this.rawEndpoint = rawJson.endpoint
    this.definition = definition
    this.rawJson = rawJson
    this.definitionString = fs.readFileSync(this.definitionPath!, 'utf-8')
    this.typesString = this.getTypesString(this.definition)
    const secrets = this.definition.secret
    this.secrets = secrets ? secrets.replace(/\s/g, '').split(',') : null
  }

  get endpoint(): string | undefined {
    return (
      (this.definition && this.definition.endpoint) ||
      process.env.PRISMA_MANAGEMENT_API_ENDPOINT
    )
  }

  get clusterBaseUrl(): string | undefined {
    if (!this.definition || !this.endpoint) {
      return undefined
    }
    const { clusterBaseUrl } = parseEndpoint(this.endpoint)
    return clusterBaseUrl
  }

  get service(): string | undefined {
    if (!this.definition) {
      return undefined
    }
    if (!this.endpoint) {
      return undefined
    }
    const { service } = parseEndpoint(this.endpoint)
    return service
  }

  get stage(): string | undefined {
    if (!this.definition) {
      return undefined
    }
    if (!this.endpoint) {
      return undefined
    }
    const { stage } = parseEndpoint(this.endpoint)
    return stage
  }

  get cluster(): string | undefined {
    if (!this.definition) {
      return undefined
    }
    if (!this.endpoint) {
      return undefined
    }
    const { clusterName } = parseEndpoint(this.endpoint)
    return clusterName
  }

  validate() {
    // shared clusters need a workspace
    const clusterName = this.getClusterName()
    const cluster = this.env.clusterByName(clusterName!)!
    if (
      this.definition &&
      clusterName &&
      cluster &&
      cluster.shared &&
      !cluster.isPrivate &&
      !this.getWorkspace() &&
      clusterName !== 'shared-public-demo'
    ) {
      throw new Error(
        `Your \`cluster\` property in the prisma.yml is missing the workspace slug.
Make sure that your \`cluster\` property looks like this: ${chalk.bold(
          '<workspace>/<cluster-name>',
        )}. You can also remove the cluster property from the prisma.yml
and execute ${chalk.bold.green(
          'prisma deploy',
        )} again, to get that value auto-filled.`,
      )
    }
    if (
      this.definition &&
      this.definition.endpoint &&
      clusterName &&
      cluster &&
      cluster.shared &&
      !cluster.isPrivate &&
      !this.getWorkspace() &&
      clusterName !== 'shared-public-demo'
    ) {
      throw new Error(
        `The provided endpoint ${
          this.definition.endpoint
        } points to a demo cluster, but is missing the workspace slug. A valid demo endpoint looks like this: https://eu1.prisma.sh/myworkspace/service-name/stage-name`,
      )
    }
    if (
      this.definition &&
      this.definition.endpoint &&
      !this.definition.endpoint.startsWith('http')
    ) {
      throw new Error(
        `${chalk.bold(
          this.definition.endpoint,
        )} is not a valid endpoint. It must start with http:// or https://`,
      )
    }
    this.env.sharedClusters
  }

  getToken(serviceName: string, stageName: string): string | undefined {
    if (this.secrets) {
      const data = {
        data: {
          service: `${serviceName}@${stageName}`,
          roles: ['admin'],
        },
      }
      return jwt.sign(data, this.secrets[0], {
        expiresIn: '7d',
      })
    }

    return undefined
  }

  getCluster(throws: boolean = false): Cluster | undefined {
    if (this.definition && this.endpoint) {
      const {
        clusterBaseUrl,
        isPrivate,
        local,
        shared,
        workspaceSlug,
        clusterName,
      } = parseEndpoint(this.endpoint)
      if (clusterBaseUrl) {
        debug('making cluster here')
        const existingCluster = this.env.clusters.find(
          c => c.baseUrl.toLowerCase() === clusterBaseUrl,
        )
        const cluster =
          existingCluster ||
          new Cluster(
            this.out!,
            clusterName,
            clusterBaseUrl,
            shared ? this.env.cloudSessionKey : undefined,
            local,
            shared,
            isPrivate,
            workspaceSlug,
          )
        this.env.removeCluster(clusterName)
        this.env.addCluster(cluster)
        return cluster
      }
    }

    return undefined
  }

  getTypesString(definition: PrismaDefinition) {
    const typesPaths = definition.datamodel
      ? Array.isArray(definition.datamodel)
        ? definition.datamodel
        : [definition.datamodel]
      : []

    const errors: ErrorMessage[] = []
    let allTypes = ''
    typesPaths.forEach(unresolvedTypesPath => {
      const typesPath = path.join(this.definitionDir, unresolvedTypesPath!)
      if (fs.existsSync(typesPath)) {
        const types = fs.readFileSync(typesPath, 'utf-8')
        allTypes += types + '\n'
      } else {
        throw new Error(
          `The types definition file "${typesPath}" could not be found.`,
        )
      }
    })

    return allTypes
  }

  getClusterName(): string | null {
    return this.cluster || null
  }

  getWorkspace(): string | null {
    if (this.definition && this.endpoint) {
      const { workspaceSlug } = parseEndpoint(this.endpoint)
      if (workspaceSlug) {
        return workspaceSlug
      }
    }

    return null
  }

  getDeployName() {
    const cluster = this.getCluster()
    return concatName(cluster!, this.service!, this.getWorkspace())
  }

  getSubscriptions(): FunctionInput[] {
    if (this.definition && this.definition.subscriptions) {
      return Object.keys(this.definition!.subscriptions!).map(name => {
        const subscription = this.definition!.subscriptions![name]

        const url =
          typeof subscription.webhook === 'string'
            ? subscription.webhook
            : subscription.webhook.url
        const headers =
          typeof subscription.webhook === 'string'
            ? []
            : transformHeaders(subscription.webhook.headers)

        let query = subscription.query
        if (subscription.query.endsWith('.graphql')) {
          const queryPath = path.join(this.definitionDir, subscription.query)
          if (!fs.pathExistsSync(queryPath)) {
            throw new Error(
              `Subscription query ${queryPath} provided in subscription "${name}" in prisma.yml does not exist.`,
            )
          }
          query = fs.readFileSync(queryPath, 'utf-8')
        }

        return {
          name,
          query,
          headers,
          url,
        }
      })
    }
    return []
  }

  replaceEndpoint(newEndpoint) {
    this.definitionString = replaceYamlValue(
      this.definitionString,
      'endpoint',
      newEndpoint,
    )
    fs.writeFileSync(this.definitionPath!, this.definitionString)
  }

  addDatamodel(datamodel) {
    this.definitionString += `\ndatamodel: ${datamodel}`
    fs.writeFileSync(this.definitionPath!, this.definitionString)
    this.definition!.datamodel = datamodel
  }

  getEndpoint(serviceInput?: string, stageInput?: string) {
    const cluster = this.getCluster()
    const service = serviceInput || this.service
    const stage = stageInput || this.stage
    const workspace = this.getWorkspace()

    if (service && stage && cluster) {
      return cluster!.getApiEndpoint(service, stage, workspace)
    }

    return null
  }

  getHooks(hookType: HookType): string[] {
    if (
      this.definition &&
      this.definition.hooks &&
      this.definition.hooks[hookType]
    ) {
      const hooks = this.definition.hooks[hookType]
      if (typeof hooks !== 'string' && !Array.isArray(hooks)) {
        throw new Error(
          `Hook ${hookType} provided in prisma.yml must be string or an array of strings.`,
        )
      }
      return typeof hooks === 'string' ? [hooks] : hooks
    }

    return []
  }
}

export function concatName(
  cluster: Cluster,
  name: string,
  workspace: string | null,
) {
  if (cluster.shared) {
    const workspaceString = workspace ? `${workspace}~` : ''
    return `${workspaceString}${name}`
  }

  return name
}

function transformHeaders(headers?: { [key: string]: string }): Header[] {
  if (!headers) {
    return []
  }
  return Object.keys(headers).map(key => ({
    name: key,
    value: headers[key],
  }))
}

export function getEndpointFromRawProps(
  clusterWorkspace: string,
  service: string,
  stage: string,
) {
  const splitted = clusterWorkspace.split('/')
  const cluster = splitted.length > 1 ? splitted[1] : splitted[0]
  const workspace = splitted.length > 1 ? splitted[0] : undefined
}
