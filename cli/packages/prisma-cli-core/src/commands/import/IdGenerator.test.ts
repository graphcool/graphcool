import IdGenerator from './IdGenerator'
import * as fs from 'fs-extra'
import * as path from 'path'
import { getTmpDir } from '../../test/getTmpDir'

describe('IdGenerator', () => {
  it('Should throw an error when passing invalid files', () => {
    const nodesPath = path.join(getTmpDir(), '000001.json')
    const data = {
      valueType: 'nodes',
      valuess: [
        {
          _typeName: 'Post',
          id: 'asdasdasdasdasd',
          title:
            'Quas dolores earum corporis aut voluptatibus dicta voluptatem.',
          description:
            'Et minima ut veritatis rerum aut architecto sint non accusamus.',
          state: 'Published',
        },
      ],
    }

    fs.writeFileSync(nodesPath, JSON.stringify(data))
    expect(() => IdGenerator.generateMissingIds([nodesPath])).toThrow()
  })

  it('Should generate ids when they are missing', () => {
    const nodesPath = path.join(
      __dirname,
      'fixtures',
      'basic',
      'import-missing-ids',
      'nodes',
    )
    const nodes = fs
      .readdirSync(nodesPath)
      .map(file => path.join(nodesPath, file))
    const nodesWithGeneratedIds = IdGenerator.generateMissingIds(nodes)

    const resultData = fs.readJsonSync(nodesWithGeneratedIds[0])
    resultData.values.forEach(node => {
      expect(!node.id).toBeFalsy()
      expect(node.id).toMatch(new RegExp('^c[a-zA-Z0-9_.-]*'))
    })
  })

  it('Should leave supplied ids untouched', () => {
    const nodesPath = path.join(
      __dirname,
      'fixtures',
      'basic',
      'import-missing-ids',
      'nodes',
    )
    const nodes = fs
      .readdirSync(nodesPath)
      .map(file => path.join(nodesPath, file))
    const nodesWithGeneratedIds = IdGenerator.generateMissingIds(nodes)

    const initialData = fs.readJsonSync(nodes[1])
    const resultData = fs.readJsonSync(nodesWithGeneratedIds[1])
    resultData.values.forEach((node, index) => {
      expect(!initialData.values[index].id).toBeFalsy()
      expect(!node.id).toBeFalsy()
      expect(node.id).toMatch(initialData.values[index].id)
    })
  })
})
