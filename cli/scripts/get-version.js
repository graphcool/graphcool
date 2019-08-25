require('cross-fetch/polyfill')

const packages = [
  'prisma',
  'prisma-client-lib',
  'prisma-cli-core',
  'prisma-cli-engine',
  'prisma-datamodel',
  'prisma-db-introspection',
  'prisma-generate-schema',
  'prisma-yml',
]

async function getVersion(branch) {
  if (branch === 'master') {
    throw new Error(
      `The get-version.js script should only be called for beta or alpha branch`,
    )
  }
  const meta = {}
  for (const package of packages) {
    meta[package] = await fetch(`https://registry.npmjs.org/${package}`).then(
      res => res.json(),
    )
  }
  const baseVersion = meta['prisma']['dist-tags']['latest']
  const latestRegex = /(\d+)\.(\d+)\.(\d+)(.*)/
  const match = latestRegex.exec(baseVersion)
  if (!match) {
    throw new Error(`Can't match latest version ${baseVersion} with regex`)
  }
  const latestMinor = parseInt(match[2], 10)
  const currentMinor = branch === 'beta' ? latestMinor + 1 : latestMinor + 2
  let highestPatch = 0
  let highestLastNumber = 0

  const versionRegex = /(\d+)\.(\d+)\.(\d+)-(beta|alpha)\.(\d+)/
  Object.keys(meta).forEach(packageName => {
    const packageMeta = meta[packageName]
    const currentVersion = packageMeta['dist-tags'][branch]
    const match = versionRegex.exec(currentVersion)
    if (!match) {
      // console.warn(
      //   `Could not match version ${currentVersion} of branch ${branch} of package ${packageName}`,
      // )
      return
    }
    const minor = parseInt(match[2], 10)
    const patch = parseInt(match[3], 10)
    const lastNumber = parseInt(match[5], 10)
    // if the last minor that this package on this tag has been deployed to is lower than
    // the latest minor +1 or +2 depending on the branch, a stable release did just happen
    // clear the lastNumber and patch in this case
    if (minor === currentMinor) {
      if (patch > highestPatch) {
        highestPatch = patch
      }
      if (lastNumber > highestLastNumber) {
        highestLastNumber = lastNumber
      }
    }
  })

  console.log(
    `1.${currentMinor}.${highestPatch}-${branch}.${highestLastNumber + 1}`,
  )
}

if (process.argv.length === 2) {
  throw new Error(
    `Please provide the branch you want to get the latest version for. \`node get-micro-number.js master\``,
  )
}

getVersion(process.argv[2])

process.on('unhandledRejection', e => onError(e))

function onError(e) {
  console.error(e)
  process.exit(1)
}
