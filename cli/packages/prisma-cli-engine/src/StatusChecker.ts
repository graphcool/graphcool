import { Config } from './Config'
import * as crypto from 'crypto'
import * as path from 'path'
import { spawn } from 'child_process'
import * as fetch from 'cross-fetch'
import * as fs from 'fs-extra'
import { Environment, getProxyAgent } from 'prisma-yml'
import * as os from 'os'
import { getIsGlobal } from './utils/isGlobal'
import * as serializeError from 'serialize-error'

export class StatusChecker {
  config: Config
  env?: Environment
  constructor(config: Config, env?: Environment) {
    this.config = config
    this.env = env
  }
  checkStatus(
    command: string,
    args: any,
    flags: any,
    argv: any[],
    error?: Error | string,
  ) {
    const source = 'CLI'
    const sourceVersion = this.config.version
    const eventName = error ? 'command_error' : 'command_triggered'
    const serializedError = error ? convertError(error) : undefined
    const payload = JSON.stringify({
      command,
      args,
      flags,
      argv,
      error: serializedError,
    })
    const auth = this.env ? this.env.cloudSessionKey : undefined
    const hashDate = new Date().toISOString()
    const mac = getMac()

    const fid = getFid()
    const globalBin = getIsGlobal()

    const message = JSON.stringify({
      source,
      sourceVersion,
      eventName,
      payload,
      auth,
      fid,
      globalBin,
      hashDate,
    })
    const secret = 'loTheeChaxaiPhahsa8Aifiel'

    const hash = crypto
      .createHmac('sha256', secret)
      .update(message)
      .digest('hex')

    const query = `mutation(
      $input: StatsInput!
    ) {
      sendStats(
        data: $input
      )
    }`

    const options = {
      request: {
        query,
        variables: {
          input: {
            source,
            sourceVersion,
            eventName,
            payload,
            auth,
            fid,
            globalBin,
            hash,
            hashDate,
          },
        },
      },
      cachePath: this.config.requestsCachePath,
    }

    // Spawn a detached process, passing the options as an environment property

    // doJobs(options.cachePath, options.request)

    spawn(
      process.execPath,
      [path.join(__dirname, 'check.js'), JSON.stringify(options)],
      {
        detached: true,
        stdio: 'ignore',
      },
    ).unref()
  }
}

let statusChecker: StatusChecker | undefined

export function initStatusChecker(
  config: Config,
  env?: Environment,
): StatusChecker {
  statusChecker = new StatusChecker(config, env)

  return statusChecker!
}

export function getStatusChecker(): StatusChecker | undefined {
  return statusChecker
}

export async function doJobs(cachePath: string, request: any) {
  // use a '\n' separated list of stringified requests to make append only possible
  fs.appendFileSync(cachePath, JSON.stringify(request) + '\n')
  const requestsString = fs.readFileSync(cachePath, 'utf-8')
  const lines = requestsString.split('\n')
  const pendingRequests = lines
    .filter(r => r.trim() !== '')
    .map(r => JSON.parse(r))
  let linesToDelete = 0
  try {
    for (const job of pendingRequests) {
      await requestWithTimeout(job)
      linesToDelete++
    }
  } catch (e) {
    //
  }

  fs.writeFileSync(cachePath, lines.slice(linesToDelete).join('\n'))
  process.exit()
}

async function requestWithTimeout(input) {
  return new Promise(async (resolve, reject) => {
    setTimeout(() => {
      reject('Timeout')
    }, 5000)
    const result = await fetch(
      process.env.STATS_ENDPOINT || 'https://stats.prismagraphql.com',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(input),
        agent: getProxyAgent('https://stats.prismagraphql.com'),
      },
    )
    const json = await result.json()
    resolve(json)
  })
}

function getMac() {
  const interfaces = os.networkInterfaces()
  return Object.keys(interfaces).reduce((acc, key) => {
    if (acc) {
      return acc
    }
    const i = interfaces[key]
    const mac = i.find(a => a.mac !== '00:00:00:00:00:00')
    return mac ? mac.mac : null
  }, null)
}

let fidCache: string | null = null

export function getFid() {
  if (fidCache) {
    return fidCache
  }
  const mac = getMac()
  const fidSecret = 'AhTheeR7Pee0haebui1viemoe'
  const fid = mac
    ? crypto
        .createHmac('sha256', fidSecret)
        .update(mac)
        .digest('hex')
    : ''
  fidCache = fid
  return fid
}

function convertError(e: Error | string) {
  if (typeof e === 'string') {
    return { message: e }
  }

  return serializeError(e)
}
