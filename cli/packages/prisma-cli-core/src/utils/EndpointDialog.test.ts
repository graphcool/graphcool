import { EndpointDialog } from './EndpointDialog'
import { Output, Config, Client } from 'prisma-cli-engine'
import { Environment, PrismaDefinitionClass } from 'prisma-yml'
import { getTmpDir } from '../test/getTmpDir'
import { normalizeDockerCompose } from '../commands/init/index.test'

function makeDialog() {
  const config = new Config()
  const output = new Output(config)
  const env = new Environment(getTmpDir(), output)
  const client = new Client(config, env, output)
  const definition = new PrismaDefinitionClass(env)

  const dialog = new EndpointDialog({
    out: output,
    client,
    env,
    config,
    definition,
    shouldAskForGenerator: false,
  })

  return dialog
}

describe('endpoint dialog', () => {
  const dialog = makeDialog()
  test('local', async () => {
    const input = {
      choice: 'local',
      loggedIn: false,
      serviceName: 'some-folder',
      localClusterRunning: false,
      existingData: false,
    }
    const result = await dialog.handleChoice(input)
    expect({ input, result: normalizeResult(result) }).toMatchSnapshot()
  })
  test('local running', async () => {
    const input = {
      choice: 'local',
      loggedIn: false,
      serviceName: 'some-folder',
      localClusterRunning: true,
      existingData: false,
    }
    const result = await dialog.handleChoice(input)
    expect({ input, result: normalizeResult(result) }).toMatchSnapshot()
  })
  test('propose service name', () => {
    const endpointDialog = makeDialog()
    expect(endpointDialog.proposeServiceName('/Users/Peter/workspace/webshop')).toBe('webshop')
    expect(endpointDialog.proposeServiceName('/Users/Peter/workspace/webshop/prisma')).toBe('webshop')
  })
})

export function normalizeResult({ dockerComposeYml, ...result }) {
  return {
    ...result,
    dockerComposeYml: normalizeDockerCompose(dockerComposeYml),
  }
}
