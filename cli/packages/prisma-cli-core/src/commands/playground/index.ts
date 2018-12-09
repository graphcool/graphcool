import { Command, flags, Flags } from 'prisma-cli-engine'
import * as opn from 'opn'
import * as fs from 'fs-extra'
const debug = require('debug')('playground')
import * as path from 'path'
import * as os from 'os'
import * as crypto from 'crypto'
import chalk from 'chalk'
import * as express from 'express'
import * as requestProxy from 'express-request-proxy'
import expressPlayground from 'graphql-playground-middleware-express'
import { getGraphQLConfig, GraphQLConfig } from 'graphql-config'
import {
  makeConfigFromPath,
  patchEndpointsToConfig,
} from 'graphql-config-extension-prisma'

function randomString(len = 32) {
  return crypto
    .randomBytes(Math.ceil(len * 3 / 4))
    .toString('base64')
    .slice(0, len)
    .replace(/\+/g, '0')
    .replace(/\//g, '0')
}

export default class Playground extends Command {
  static topic = 'playground'
  static description = 'Open service endpoints in GraphQL Playground'
  static group = 'general'
  static flags: Flags = {
    web: flags.boolean({
      char: 'w',
      description: 'Force open web playground',
    }),
    'env-file': flags.string({
      description: 'Path to .env file to inject env vars',
      char: 'e',
    }),
    'server-only': flags.boolean({
      char: 's',
      description: 'Run only the server',
    }),
    port: flags.number({
      char: 'p',
      description: 'Port to serve the Playground web version on. Assumes --web.',
    }),
  }
  async run() {
    let { web, port } = this.flags

    // port assumes web. using default value of flags will break this!
    if (port) {
      web = true
    }

    // set default value, don't overwrite
    if (!port) {
      port = 3000
    }

    const envFile = this.flags['env-file']
    const serverOnly = this.flags['server-only']
    await this.definition.load(this.flags, envFile)

    const stage = this.definition.stage!
    const cluster = this.definition.getCluster()

    const localPlaygroundPath = `/Applications/GraphQL\ Playground.app/Contents/MacOS/GraphQL\ Playground`

    const isLocalPlaygroundAvailable = fs.existsSync(localPlaygroundPath)

    const shouldStartServer = serverOnly || web || !isLocalPlaygroundAvailable

    const shouldOpenBrowser = !serverOnly

    const config = await this.getConfig()

    if (shouldStartServer) {
      const endpoint = this.definition.definition!.endpoint || cluster!.getApiEndpoint(
        this.definition.service!,
        stage,
        this.definition.getWorkspace() || undefined,
      )
      const link = await this.startServer({ config, endpoint, port })

      if (shouldOpenBrowser) {
        opn(link).catch(() => {}); // Prevent `unhandledRejection` error.
      }
    } else {
      const envPath = path.join(os.tmpdir(), `${randomString()}.json`)
      fs.writeFileSync(envPath, JSON.stringify(process.env))
      const url = `graphql-playground://?cwd=${process.cwd()}&envPath=${envPath}`
      opn(url, { wait: false })
    }
  }

  async getConfig(): Promise<GraphQLConfig> {
    try {
      return await patchEndpointsToConfig(
        getGraphQLConfig(this.config.cwd),
        this.config.cwd,
      )
    } catch (e) {
      return makeConfigFromPath() as any
    }
  }

  startServer = async ({
    config,
    endpoint,
    port = 3000,
  }: {
    config
    endpoint: string
    port: number
  }) =>
    new Promise<string>(async (resolve, reject) => {
      const app = express()
      const projects = config.getProjects()

      if (projects === undefined) {
        const { url, headers } = config.endpointsExtension.getEndpoint(endpoint)

        app.use(
          '/graphql',
          requestProxy({
            url,
            headers,
          }),
        )

        app.use(
          '/playground',
          expressPlayground({
            endpoint: '/graphql',
            config: config.config,
          }),
        )
      } else {
        app.use(
          '/playground',
          expressPlayground({ config: config.config } as any),
        )
      }

      const listener = app.listen(port, () => {
        let host = listener.address().address
        if (host === '::') {
          host = 'localhost'
        }
        const link = `http://${host}:${port}/playground`
        console.log('Serving playground at %s', chalk.blue(link))

        resolve(link)
      })
    })
}
