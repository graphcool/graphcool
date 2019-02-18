import * as cuid from 'scuid'
import * as fs from 'fs-extra'
import * as os from 'os'
import * as path from 'path'
import { Files } from './Importer'
import { ImportData } from './types'

export default class IdGenerator {
  static generateMissingIds(nodes: string[]) {
    const nodesWithGeneratedIds: string[] = new Array()
    for (const fileName of nodes) {
      nodesWithGeneratedIds.push(this.generateFileWithMissingIds(fileName))
    }
    return nodesWithGeneratedIds
  }

  private static generateFileWithMissingIds(fileName: string): string {
    const file = fs.readFileSync(fileName, 'utf-8')
    const data = JSON.parse(file)
    if (!data.values) {
      throw new Error('Import data is missing the "values" property')
    }
    data.values.forEach(node => {
      if (!node.id) {
        node.id = cuid()
      }
    })
    const fileWithGeneratedIds = path.join(os.tmpdir(), path.basename(fileName))
    fs.writeFileSync(fileWithGeneratedIds, JSON.stringify(data), {
      encoding: 'utf-8',
    })
    return fileWithGeneratedIds
  }
}
