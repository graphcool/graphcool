import { Engine } from '../src/Engine'

async function run() {
  const engine = new Engine()
  console.log('Starting Engine')
  await engine.start()
  console.log('Engine started')
  const result = await engine.request(
    `
    {
      artist(where: {ArtistId: 4}) {
        id
        Name
      }
    }
  `,
  )

  console.log({ result })
  engine.stop()
}

run().catch(e => console.error(e))
