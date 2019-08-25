import { Client, Config, Output } from 'prisma-cli-engine'
import { PrismaDefinitionClass } from 'prisma-yml'
import { Importer } from '../import/Importer'
import * as fs from 'fs-extra'
import * as path from 'path'
import * as childProcess from 'child_process'
import chalk from 'chalk'
import { parse, print } from 'graphql'
import * as crossSpawn from 'cross-spawn'
const debug = require('debug')('Seeder')

export class Seeder {
  definition: PrismaDefinitionClass
  client: Client
  out: Output
  config: Config
  constructor(
    definition: PrismaDefinitionClass,
    client: Client,
    out: Output,
    config: Config,
  ) {
    this.definition = definition
    this.client = client
    this.out = out
    this.config = config
  }

  async seed(
    serviceName: string,
    stageName: string,
    reset: boolean = false,
    workspaceSlug?: string,
  ) {
    const seed = this.definition.definition!.seed
    if (!seed) {
      throw new Error(
        `In order to seed, you need to provide a "seed" property in your prisma.yml`,
      )
    }
    if (seed.import && seed.run) {
      throw new Error(
        `Please provider either seed.import or seed.run but not both at the same time`,
      )
    }

    if (seed.import) {
      let source = seed.import

      if (!path.isAbsolute(seed.import)) {
        source = path.join(this.config.definitionDir, seed.import)
      }

      debug(source)

      if (!source.endsWith('.zip') && !source.endsWith('.graphql')) {
        throw new Error(`Source must end with .zip or .graphql`)
      }

      if (!fs.pathExistsSync(source)) {
        throw new Error(`Path ${source} does not exist`)
      }

      const token = this.definition.getToken(serviceName, stageName)

      if (reset) {
        await this.reset(serviceName, stageName)
      }

      if (source.endsWith('.zip')) {
        await this.import(source, serviceName, stageName, token, workspaceSlug)
      } else if (source.endsWith('.graphql')) {
        await this.executeQuery(
          source,
          serviceName,
          stageName,
          token,
          workspaceSlug,
        )
      }
    }

    if (seed.run) {
      if (reset) {
        await this.reset(serviceName, stageName)
      }

      await this.run(seed.run)
    }
  }

  getOperations(query) {
    const ast = parse(query)
    return ast.definitions
      .filter(d => d.kind === 'OperationDefinition')
      .map(d => {
        return print({
          kind: 'Document',
          definitions: [d],
        })
      })
  }

  async executeQuery(
    filePath: string,
    serviceName: string,
    stageName: string,
    token?: string,
    workspaceSlug?: string,
  ) {
    if (!fs.pathExistsSync(filePath)) {
      throw new Error(`Can't find seed import file ${filePath}`)
    }

    const query = fs.readFileSync(filePath, 'utf-8')
    try {
      const operations = this.getOperations(query)

      try {
        for (const operation of operations) {
          await this.client.exec(
            serviceName,
            stageName,
            operation,
            token,
            workspaceSlug,
          )
        }
      } catch (e) {
        throw new Error(`Error while executing operation:\n${e.message}`)
      }
    } catch (e) {
      throw new Error(`Error while parsing ${filePath}:\n${e.message}`)
    }
  }

  async reset(serviceName, stageName) {
    const before = Date.now()
    this.out.action.start(
      `Resetting ${chalk.bold(`${serviceName}@${stageName}`)}`,
    )
    await this.client.reset(
      serviceName,
      stageName,
      this.definition.getToken(serviceName, stageName),
    )
    this.out.action.stop(chalk.cyan(`${Date.now() - before}ms`))
  }

  private run(cmd: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const args = cmd.split(/\s/g)
      const child = crossSpawn(args[0], args.slice(1), {
        cwd: this.config.cwd,
      })
      child.stdout.on('data', data => {
        this.out.log(data.toString())
      })
      child.stderr.on('data', data => {
        this.out.log(data.toString())
      })
      child.on('error', err => {
        this.out.error(err)
      })
      child.on('close', code => {
        if (code !== 0) {
          reject(code)
        } else {
          resolve()
        }
      })
    })
  }

  private async import(
    source: string,
    serviceName: string,
    stage: string,
    token?: string,
    workspaceSlug?: string,
  ) {
    await this.definition.load({})
    const typesString = this.definition.typesString!
    const importer = new Importer(
      source,
      typesString,
      this.client,
      this.out,
      this.config,
    )
    await importer.upload(serviceName, stage, token, workspaceSlug)
  }
}
