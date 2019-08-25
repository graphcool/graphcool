const debug = require('debug')('environment')
import 'cross-fetch/polyfill'
import * as jwt from 'jsonwebtoken'
import { cloudApiEndpoint } from './constants'
import { GraphQLClient } from 'graphql-request'
import chalk from 'chalk'
import { isLocal } from './Environment'
import { IOutput } from './Output'
import { getProxyAgent } from './utils/getProxyAgent'

export class Cluster {
  name: string
  baseUrl: string
  local: boolean
  shared: boolean
  clusterSecret?: string
  requiresAuth: boolean
  out: IOutput
  isPrivate: boolean
  workspaceSlug?: string
  private cachedToken?: string
  hasOldDeployEndpoint: boolean
  custom?: boolean
  constructor(
    out: IOutput,
    name: string,
    baseUrl: string,
    clusterSecret?: string,
    local: boolean = true,
    shared: boolean = false,
    isPrivate: boolean = false,
    workspaceSlug?: string,
  ) {
    this.out = out
    this.name = name

    // All `baseUrl` extension points in this class
    // adds a trailing slash. Here we remove it from
    // the passed `baseUrl` in order to avoid double
    // slashes.
    this.baseUrl = baseUrl.replace(/\/$/, '')
    this.clusterSecret = clusterSecret
    this.local = local
    this.shared = shared
    this.isPrivate = isPrivate
    this.workspaceSlug = workspaceSlug
    this.hasOldDeployEndpoint = false
  }

  async getToken(
    serviceName: string,
    workspaceSlug?: string,
    stageName?: string,
  ): Promise<string | null> {
    // public clusters just take the token

    const needsAuth = await this.needsAuth()
    debug({needsAuth})
    if (!needsAuth) {
      return null
    }

    if (this.name === 'shared-public-demo') {
      return ''
    }
    if (this.isPrivate && process.env.PRISMA_MANAGEMENT_API_SECRET) {
      return this.getLocalToken()
    }
    if (this.shared || (this.isPrivate && !process.env.PRISMA_MANAGEMENT_API_SECRET)) {
      return this.generateClusterToken(serviceName, workspaceSlug, stageName)
    } else {
      return this.getLocalToken()
    }
  }

  getLocalToken(): string | null {
    if (!this.clusterSecret && !process.env.PRISMA_MANAGEMENT_API_SECRET) {
      return null
    }
    if (!this.cachedToken) {
      const grants = [{ target: `*/*`, action: '*' }]
      const secret =
        process.env.PRISMA_MANAGEMENT_API_SECRET || this.clusterSecret

      try {
        const algorithm = process.env.PRISMA_MANAGEMENT_API_SECRET
          ? 'HS256'
          : 'RS256'
        this.cachedToken = jwt.sign({ grants }, secret, {
          expiresIn: '5y',
          algorithm,
        })
      } catch (e) {
        throw new Error(
          `Could not generate token for cluster ${chalk.bold(
            this.getDeployEndpoint(),
          )}. Did you provide the env var PRISMA_MANAGEMENT_API_SECRET?
Original error: ${e.message}`,
        )
      }
    }

    return this.cachedToken!
  }

  get cloudClient() {
    return new GraphQLClient(cloudApiEndpoint, {
      headers: {
        Authorization: `Bearer ${this.clusterSecret}`,
      },
      agent: getProxyAgent(cloudApiEndpoint),
    } as any)
  }

  async generateClusterToken(
    serviceName: string,
    workspaceSlug: string = this.workspaceSlug || '*',
    stageName?: string,
  ): Promise<string> {
    const query = `
      mutation ($input: GenerateClusterTokenRequest!) {
        generateClusterToken(input: $input) {
          clusterToken
        }
      }
    `

    const {
      generateClusterToken: { clusterToken },
    } = await this.cloudClient.request<{
      generateClusterToken: {
        clusterToken: string
      }
    }>(query, {
      input: {
        workspaceSlug,
        clusterName: this.name,
        serviceName,
        stageName,
      },
    })

    return clusterToken
  }

  async addServiceToCloudDBIfMissing(
    serviceName: string,
    workspaceSlug: string = this.workspaceSlug!,
    stageName?: string,
  ): Promise<boolean> {
    const query = `
      mutation ($input: GenerateClusterTokenRequest!) {
        addServiceToCloudDBIfMissing(input: $input)
      }
    `

    const serviceCreated = await this.cloudClient.request<{
      addServiceToCloudDBIfMissing: boolean
    }>(query, {
      input: {
        workspaceSlug,
        clusterName: this.name,
        serviceName,
        stageName,
      },
    })

    return serviceCreated.addServiceToCloudDBIfMissing
  }

  getApiEndpoint(
    service: string,
    stage: string,
    workspaceSlug?: string | null,
  ) {
    if (!this.shared && service === 'default' && stage === 'default') {
      return this.baseUrl
    }
    if (!this.shared && stage === 'default') {
      return `${this.baseUrl}/${service}`
    }
    if (this.isPrivate || this.local) {
      return `${this.baseUrl}/${service}/${stage}`
    }
    const workspaceString = workspaceSlug ? `${workspaceSlug}/` : ''
    return `${this.baseUrl}/${workspaceString}${service}/${stage}`
  }

  getWSEndpoint(service: string, stage: string, workspaceSlug?: string | null) {
    return this.getApiEndpoint(service, stage, workspaceSlug).replace(
      /^http/,
      'ws',
    )
  }

  getImportEndpoint(
    service: string,
    stage: string,
    workspaceSlug?: string | null,
  ) {
    return this.getApiEndpoint(service, stage, workspaceSlug) + `/import`
  }

  getExportEndpoint(
    service: string,
    stage: string,
    workspaceSlug?: string | null,
  ) {
    return this.getApiEndpoint(service, stage, workspaceSlug) + `/export`
  }

  getDeployEndpoint() {
    return `${this.baseUrl}/${
      this.hasOldDeployEndpoint ? 'cluster' : 'management'
    }`
  }

  async isOnline(): Promise<boolean> {
    const version = await this.getVersion()
    return typeof version === 'string'
  }

  async getVersion(): Promise<string | null> {
    // first try new api
    try {
      const result = await this.request(`{
        serverInfo {
          version
        }
      }`)

      const res = await result.json()
      const { data, errors } = res
      if (
        errors &&
        errors[0].code === 3016 &&
        errors[0].message.includes('management@default')
      ) {
        this.hasOldDeployEndpoint = true
        return this.getVersion()
      }
      if (data && data.serverInfo) {
        return data.serverInfo.version
      }
    } catch (e) {
      debug(e)
    }

    // if that doesn't work, try the old one
    try {
      const result = await this.request(`{
        serverInfo {
          version
        }
      }`)

      const res = await result.json()
      const { data, errors } = res
      return data.serverInfo.version
    } catch (e) {
      debug(e)
    }

    return null
  }

  request(query: string, variables?: any) {
    return fetch(this.getDeployEndpoint(), {
      method: 'post',
      headers: {
        'Content-Type': 'application/json',
      } as any,
      body: JSON.stringify({
        query,
        variables,
      }),
      agent: getProxyAgent(this.getDeployEndpoint()),
    } as any)
  }

  async needsAuth(): Promise<boolean> {
    try {
      const result = await this.request(`{
        listProjects {
          name
        }
      }`)
      const data = await result.json()
      if (data.errors && data.errors.length > 0) {
        return true
      }
      return false
    } catch (e) {
      debug('Assuming that the server needs authentication')
      debug(e.toString())
      return true
    }
  }

  toJSON() {
    return {
      name: this.name,
      baseUrl: this.baseUrl,
      local: this.local,
      clusterSecret: this.clusterSecret,
      shared: this.shared,
      isPrivate: this.isPrivate,
      workspaceSlug: this.workspaceSlug,
    }
  }
}
