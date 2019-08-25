require('cross-fetch/polyfill')

async function waitUntilPublished(tag) {
  const res = await fetch(
    `https://hub.docker.com/v2/repositories/prismagraphql/prisma/tags/?page=1&page_size=250`,
  )
  const data = await res.json()
  const foundTag = data.results.find(r => r.name === tag)
  if (!foundTag) {
    console.log(
      `Tag ${tag} is not yet published on docker hub. Waiting 10s and trying again...`,
    )
    await new Promise(r => setTimeout(r, 10000))
    return waitUntilPublished(tag)
  }
  console.log(`Tag ${tag} is published on docker hub.`)
}

if (process.argv.length === 2) {
  throw new Error(
    `Please provide the tag you want to check. \`node waitUntilTagPublished.js 1.0.1\``,
  )
}

waitUntilPublished(process.argv[2])

process.on('unhandledRejection', e => onError(e))

function onError(e) {
  console.error(e)
  process.exit(1)
}
