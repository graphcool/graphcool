import { Command, flags, Flags } from 'prisma-cli-engine'
import * as fs from 'fs-extra'
import * as path from 'path'
import * as dotenv from 'dotenv'
import * as jwt from 'jsonwebtoken'
import * as ncp from 'copy-paste'
export default class ManagementToken extends Command {
  static topic = 'management-token'
  static description = 'Create a new management API token'
  static flags: Flags = {
    secret: flags.string({
      description: 'Management API secret',
      char: 's',
    }),
    ['env-file']: flags.string({
      description: 'Path to .env file to inject env vars',
      char: 'e',
    }),
    ['expiry-duration']: flags.number({
      description: 'An expiry duration in hours',
      defaultValue: 1,
      char: 'd',
    }),
    copy: flags.boolean({
      description: 'Copy token to clipboard',
      char: 'c',
    }),
  }
  async run() {
    const { secret, copy } = this.flags
    const envFile = this.flags['env-file']
    const expDuration = this.flags['expiry-duration']
    const tokenPayload = {
      grants: [
        {
          target: '*/*',
          action: '*',
        },
      ],
    }
    if (!secret && !envFile) {
      throw new Error('You must specify either --env-file or --secret')
    } else {
      if (envFile) {
        this.loadEnvFile(envFile)
      }
      const token = jwt.sign(
        tokenPayload,
        secret ? secret : process.env.PRISMA_MANAGEMENT_API_SECRET,
        { expiresIn: `${expDuration}h`}
      )
      if (!copy) {
        this.out.log(token)
      } else {
        ncp.copy(token, () => {
          this.out.log('Management api token was copied to the clipboard')
        })
      }
    }
  }
  loadEnvFile(envFilePath) {
    if (!fs.pathExistsSync(envFilePath)) {
      envFilePath = path.join(process.cwd(), envFilePath)
    }
    if (!fs.pathExistsSync(envFilePath)) {
      throw new Error(`--env-file path '${envFilePath}' does not exist`)
      
    }
    dotenv.config({ path: envFilePath })
    if(!process.env.PRISMA_MANAGEMENT_API_SECRET) {
      throw new Error('Cannot find variable PRISMA_MANAGEMENT_API_SECRET in environment file')
    }
  }
}
