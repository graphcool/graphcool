## Prisma Native Image
This repository contains the latest binary for MacOS and instructions on how to use it.

## Setup
1. Clone this repo.
1. Unpack the binary.
1. Start a postgres instance/container or use the `docker-compose.yml` from this repo to start one with `docker-compose up -d`
1. If you use a different postgres, change the `prisma.yml` to reflect your setup.
1. `direnv allow`
1. `./prisma-native`

## Known limitations & errors
- The binary will likely crash from time to time with a massive thread dump, if that happens send @dpetrick a message in slack _with the whole thing_.
- UUID type is most likely not working
- Authentication is disabled at the moment.
- Logging is a mess and fairly verbose. There's some control with `LOG_LEVEL=[TRACE, DEBUG, INFO, ERROR, WARN]`, but not all logging is affected by that at the moment.

## If you encounter things that don't work
- Open an issue here