import ManagementToken from "./";
import * as fs from 'fs-extra'
import * as jwt from 'jsonwebtoken'
import { getTmpDir } from '../../test/getTmpDir'

describe('ManagementToken', () => {
    const tmpDir = getTmpDir()
    fs.writeFileSync(`${tmpDir}/envFile`,'PRISMA_MANAGEMENT_API_SECRET=anotherSecret')

    it('should create a management api token with the given secret', async () => {
        const result = await ManagementToken.mock('-s', 'mySecret')
        const token = result.out.stdout.output.trim()
        expect(token).toMatch(/^eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9/)
        const payload = jwt.verify(token, 'mySecret')
        const nowInSec = Math.floor(new Date().getTime() / 1000)
        expect(payload).toHaveProperty('iat', nowInSec)
    })
    it('should create a management api token with the given env file', async () => {
        const result = await ManagementToken.mock('--env-file', `${tmpDir}/envFile`)
        const token = result.out.stdout.output.trim()
        expect(token).toMatch(/^eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9/)
        const payload = jwt.verify(token, 'anotherSecret')
        const nowInSec = Math.floor(new Date().getTime() / 1000)
        expect(payload).toHaveProperty('iat', nowInSec)
    })
    it('should create a management api token with a custom expiry duration', async () => {
        const expiryDuration = 5
        const result = await ManagementToken.mock('-s', 'mySecret', '-d', expiryDuration)
        const token = result.out.stdout.output.trim()
        expect(token).toMatch(/^eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9/)
        const payload = jwt.verify(token, 'mySecret')
        const nowInSec = Math.floor(new Date().getTime() / 1000)
        expect(payload).toHaveProperty('iat', nowInSec)
        expect(payload).toHaveProperty('exp', nowInSec + (3600*expiryDuration))
    })
})