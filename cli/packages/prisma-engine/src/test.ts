import { Engine } from './Engine'

async function run() {
  const engine = new Engine()
  console.log('Starting Engine')
  await engine.start()
  console.log('Engine started')
  const result = await engine.request(
    `
    {
      cats {
        id
      }
    }
  `,
  )

  console.log({ result })
  engine.stop()
}

run().catch(e => console.error(e))
