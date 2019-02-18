import * as path from 'path'
import * as fs from 'fs-extra'
import { Client, Output, Config } from 'prisma-cli-engine'
import * as globby from 'globby'
import { Validator } from './Validator'
import IdGenerator from './IdGenerator'
import chalk from 'chalk'
import * as AdmZip from 'adm-zip'
import * as figures from 'figures'

export interface Files {
  lists: string[]
  nodes: string[]
  relations: string[]
}

export interface State {
  lists: number
  nodes: number
  relations: number
}

const defaultState = {
  nodes: 0,
  lists: 0,
  relations: 0,
}

export class Importer {
  importPath: string
  importDir: string
  client: Client
  types: string
  out: Output
  statePath: string
  config: Config
  isDir: boolean
  constructor(
    importPath: string,
    types: string,
    client: Client,
    out: Output,
    config: Config,
  ) {
    if (!fs.pathExistsSync(importPath)) {
      throw new Error(`Import path ${importPath} does not exist`)
    }
    this.config = config
    this.importPath = importPath
    this.isDir = fs.lstatSync(importPath).isDirectory()
    this.importDir = this.isDir ? importPath : path.join(config.cwd, '.import/')
    this.client = client
    this.types = types
    this.out = out
    this.statePath = path.join(this.importDir, 'state.json')
  }
  saveState(state) {
    fs.writeFileSync(this.statePath, JSON.stringify(state))
  }
  getState(): State {
    try {
      const json = fs.readJsonSync(this.statePath)
      return {
        ...defaultState,
        ...json,
      }
    } catch (e) {
      //
    }

    return defaultState
  }
  getNumber(fileName: string) {
    const fileRegex = /.*?(\d+)\.json/
    const match = fileName.match(fileRegex)
    if (match) {
      return parseInt(match[1], 10)
    }

    return 0
  }
  unzip(): void {
    const before = Date.now()
    this.out.action.start('Unzipping')

    const zip = new AdmZip(this.importPath)
    zip.extractAllTo(this.importDir)

    this.out.action.stop(chalk.cyan(`${Date.now() - before}ms`))
  }
  checkForErrors(result: any) {
    if (!Array.isArray(result) && result.errors) {
      throw new Error(JSON.stringify(result, null, 2))
    }
  }
  async upload(
    serviceName: string,
    stage: string,
    token?: string,
    workspaceSlug?: string,
    generateMissingIds?: boolean,
  ) {
    try {
      if (!this.isDir) {
        this.unzip()
      }
      let before = Date.now()
      this.out.action.start('Validating data')
      const files = await this.getFiles()
      if (generateMissingIds) {
        files.nodes = IdGenerator.generateMissingIds(files.nodes)
      }
      this.validateFiles(files)
      this.out.action.stop(chalk.cyan(`${Date.now() - before}ms`))
      before = Date.now()
      this.out.log('\nUploading nodes...')
      const state = this.getState()

      for (const fileName of files.nodes) {
        const n = this.getNumber(fileName)
        if (state.nodes >= n) {
          this.out.log(`Skipping file ${fileName} (already imported)`)
          continue
        }
        const file = fs.readFileSync(fileName, 'utf-8')
        const json = JSON.parse(file)
        const result = await this.client.upload(
          serviceName,
          stage,
          file,
          token,
          workspaceSlug,
        )
        this.checkForErrors(result)
        if (result.length > 0) {
          this.out.log(this.out.getStyledJSON(result))
          this.out.exit(1)
        }

        state.nodes = n
        this.saveState(state)
      }
      this.out.log(
        'Uploading nodes done ' + chalk.cyan(`${Date.now() - before}ms`),
      )
      before = Date.now()
      this.out.log('\nUploading lists')
      for (const fileName of files.lists) {
        const n = this.getNumber(fileName)
        if (state.lists >= n) {
          this.out.log(`Skipping file ${fileName} (already imported)`)
          continue
        }
        const file = fs.readFileSync(fileName, 'utf-8')
        const json = JSON.parse(file)
        const result = await this.client.upload(
          serviceName,
          stage,
          file,
          token,
          workspaceSlug,
        )
        this.checkForErrors(result)
        if (result.length > 0) {
          this.out.log(this.out.getStyledJSON(result))
          this.out.exit(1)
        }
        state.lists = n
        this.saveState(state)
      }
      this.out.log(
        'Uploading lists done ' + chalk.cyan(`${Date.now() - before}ms`),
      )
      before = Date.now()
      this.out.log('\nUploading relations')
      for (const fileName of files.relations) {
        const n = this.getNumber(fileName)
        if (state.relations >= n) {
          this.out.log(`Skipping file ${fileName} (already imported)`)
          continue
        }
        const file = fs.readFileSync(fileName, 'utf-8')
        const json = JSON.parse(file)
        const result = await this.client.upload(
          serviceName,
          stage,
          file,
          token,
          workspaceSlug,
        )
        this.checkForErrors(result)
        if (result.length > 0) {
          this.out.log(this.out.getStyledJSON(result))
          this.out.exit(1)
        }
        state.relations = n
        this.saveState(state)
      }
      this.saveState(defaultState)
      this.out.log(
        'Uploading relations done ' + chalk.cyan(`${Date.now() - before}ms`),
      )
      if (!this.isDir) {
        fs.removeSync(this.importDir)
      }
    } catch (e) {
      this.out.log(chalk.yellow(`Uncaught exception, cleaning up: ${e}`))
      this.out.action.stop(chalk.red(figures.cross))
      if (!this.isDir) {
        fs.removeSync(this.importDir)
      }
    }
  }

  validateFiles(files: Files) {
    const validator = new Validator(this.types)

    if (
      (!files.nodes || files.nodes.length === 0) &&
      (!files.lists || files.lists.length === 0) &&
      (!files.relations || files.relations.length === 0)
    ) {
      throw new Error(
        `'Folder 'folder' does not contain any of these folders: 'nodes', 'lists', 'relations'. Read more about data import here: https://bit.ly/prisma-import-ndf'`,
      )
    }

    for (const fileName of files.nodes) {
      const file = fs.readFileSync(fileName, 'utf-8')
      const json = JSON.parse(file)
      validator.validateImportData(json)
    }
    for (const fileName of files.lists) {
      const file = fs.readFileSync(fileName, 'utf-8')
      const json = JSON.parse(file)
      validator.validateImportData(json)
    }
    for (const fileName of files.relations) {
      const file = fs.readFileSync(fileName, 'utf-8')
      const json = JSON.parse(file)
      validator.validateImportData(json)
    }
  }

  getFiles(): Files {
    return {
      lists: globby
        .sync('*.json', {
          cwd: path.join(this.importDir, 'lists/'),
        })
        .map(p => path.join(this.importDir, 'lists/', p)),
      nodes: globby
        .sync('*.json', {
          cwd: path.join(this.importDir, 'nodes/'),
        })
        .map(p => path.join(this.importDir, 'nodes/', p)),
      relations: globby
        .sync('*.json', {
          cwd: path.join(this.importDir, 'relations/'),
        })
        .map(p => path.join(this.importDir, 'relations/', p)),
    }
  }
}
