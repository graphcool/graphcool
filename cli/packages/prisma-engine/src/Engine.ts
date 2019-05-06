import { spawn, ChildProcess } from 'child_process'
import { request } from 'graphql-request'
import * as fs from 'fs'
import * as path from 'path'
import { safeLoad } from 'js-yaml'
import * as getPort from 'get-port'
import fetch from 'node-fetch'

interface EngineConfig {
  prismaConfig?: string
  debug?: boolean
  managementApiEnabled?: boolean
  datamodelJson?: string
}

/**
 * Node.js based wrapper to run the Prisma binary
 */
export class Engine {
  prismaConfig: string
  port?: number
  debug: boolean
  child?: ChildProcess
  /**
   * exiting is used to tell the .on('exit') hook, if the exit came from our script.
   * As soon as the Prisma binary returns a correct return code (like 1 or 0), we don't need this anymore
   */
  exiting: boolean = false
  managementApiEnabled: boolean = false
  datamodelJson?: string
  constructor({
    prismaConfig,
    debug,
    managementApiEnabled,
    datamodelJson,
  }: EngineConfig = {}) {
    this.prismaConfig = prismaConfig || this.getPrismaYml()
    this.debug = debug || false
    this.managementApiEnabled = managementApiEnabled || false
    this.datamodelJson = datamodelJson
  }

  /**
   * Resolve the prisma.yml
   */
  getPrismaYml() {
    const prismaYmlPath = this.getPrismaYmlPath()
    return fs.readFileSync(prismaYmlPath, 'utf-8')
  }

  getDatamodelPath() {
    const yml = safeLoad(this.prismaConfig)
    return path.resolve(yml.datamodel)
  }

  /**
   * Resolve the prisma.yml path
   */
  getPrismaYmlPath() {
    if (fs.existsSync('prisma.yml')) {
      return 'prisma.yml'
    }

    const prismaPath = path.join(process.cwd(), 'prisma/prisma.yml')
    if (fs.existsSync(prismaPath)) {
      return prismaPath
    }

    const parentPath = path.join(process.cwd(), '../prisma.yml')
    if (fs.existsSync(parentPath)) {
      return parentPath
    }

    throw new Error(`Could not find prisma.yml`)
  }

  /**
   * Replace the port in the Prisma Config
   */
  generatePrismaConfig() {
    return `managementApiEnabled: ${this.managementApiEnabled}\nport: ${
      this.port
    }\n${this.prismaConfig}`
  }

  /**
   * Starts the engine
   */
  async start() {
    // this.port = await getPort({ port: [4467, 4468, 4469, 4470, 4471] })
    this.port = 8000
    const PRISMA_CONFIG = this.generatePrismaConfig()
    if (this.debug) {
      console.log('PRISMA_CONFIG:\n')
      console.log(PRISMA_CONFIG)
      console.log('\n')
    }
    const schemaEnv: any = {}
    if (this.datamodelJson) {
      schemaEnv.PRISMA_INTERNAL_DATA_MODEL_JSON = this.datamodelJson
    } else {
      schemaEnv.SCHEMA_INFERRER_PATH = path.join(
        __dirname,
        '../schema-inferrer-bin',
      )
    }
    this.child = spawn(path.join(__dirname, '../prisma'), [], {
      env: {
        PRISMA_CONFIG,
        PRISMA_DATA_MODEL_PATH: this.getDatamodelPath(),
        SERVER_ROOT: process.cwd(),
        ...schemaEnv,
      },
      detached: false,
      stdio: this.debug ? 'inherit' : 'pipe',
    })
    this.child.on('error', e => {
      throw e
    })
    this.child.on('exit', code => {
      if (code !== 0 && !this.exiting) {
        const debugString = this.debug
          ? ''
          : 'Please enable "debug": true in the Engine constructor to get more insights.'
        throw new Error(`Child exited with code ${code}${debugString}`)
      }
    })
    await new Promise(r => setTimeout(r, 150))
  }

  /**
   * If Prisma runs, stop it
   */
  stop() {
    if (this.child) {
      this.exiting = true
      this.child.kill()
    }
  }

  /**
   *
   * @param url relative url like /service/stage
   * @param query query to send to the prisma service
   * @param variables optional: variables
   */
  async request(query, variables?: any, url = '') {
    // return request(`http://localhost:${this.port}${url}`, query, variables)
    const result = await fetch(`http://localhost:${this.port}${url}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query,
        variables: variables || {},
        operationName: '',
      }),
    }).then(res => {
      if (!res.ok) {
        return res.text()
      } else {
        return res.json()
      }
    })
    if (typeof result === 'string' || result.error) {
      throw new Error(JSON.stringify(result))
    }
    return result.data
  }
}
