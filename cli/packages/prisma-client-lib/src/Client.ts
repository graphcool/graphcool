import { ClientOptions, Exists, Model } from './types'
import {
  GraphQLObjectType,
  GraphQLScalarType,
  Kind,
  OperationTypeNode,
  print,
  GraphQLField,
  GraphQLSchema,
  buildSchema,
  GraphQLEnumType,
} from 'graphql'
import { connectionNodeHasScalars } from './utils/connectionNodeHasScalars'
import mapAsyncIterator from './utils/mapAsyncIterator'
import { mapValues } from './utils/mapValues'
import gql from 'graphql-tag'
import { getTypesAndWhere } from './utils'
const log = require('debug')('binding')
import { sign } from 'jsonwebtoken'
import { BatchedGraphQLClient } from 'batched-graphql-request'
import { SubscriptionClient } from 'subscriptions-transport-ws'
import { observableToAsyncIterable } from './utils/observableToAsyncIterable'
import * as WS from 'ws'
// to make the TS compiler happy

let instructionId = 0

export interface InstructionsMap {
  [key: string]: Array<Instruction>
}

export interface InstructionPromiseMap {
  [key: string]: Promise<any>
}

export interface Instruction {
  fieldName: string
  args?: any
  field?: GraphQLField<any, any>
  typeName: string
  fragment?: string
}

export class Client {
  /**
   * These properties are exposed through the Client API
   */
  query: any
  mutation: any
  $subscribe: any
  $graphql: any
  $exists: any
  /**
   * These are internal properties used by the Client implementation
   */
  _types: any
  _debug
  _endpoint: string
  _secret?: string
  _client: BatchedGraphQLClient
  _subscriptionClient: SubscriptionClient
  _schema: GraphQLSchema
  _token: string
  _currentInstructions: InstructionsMap = {}
  _models: Model[] = []
  _promises: InstructionPromiseMap = {}

  constructor({ typeDefs, endpoint, secret, debug, models }: ClientOptions) {
    this._debug = debug
    this._schema = buildSchema(typeDefs)
    this._endpoint = endpoint
    this._secret = secret
    this._models = models

    this.buildMethods()

    const token = secret ? sign({}, secret!) : undefined

    this.$graphql = this.buildGraphQL()
    this.$exists = this.buildExists()
    this._token = token
    this._client = new BatchedGraphQLClient(endpoint, {
      headers: token
        ? {
            Authorization: `Bearer ${token}`,
          }
        : {},
    })
    this._subscriptionClient = new SubscriptionClient(
      endpoint.replace(/^http/, 'ws'),
      {
        connectionParams: {
          Authorization: `Bearer ${token}`,
        },
        inactivityTimeout: 60000,
        lazy: true,
        reconnect: true,
      },
      WS,
    )
  }

  getOperation(instructions) {
    return instructions[0].typeName.toLowerCase()
  }

  getDocumentForInstructions(id: number) {
    log('process instructions')
    const instructions = this._currentInstructions[id]

    const { ast } = this.generateSelections(instructions)
    log('generated selections')
    const { variableDefinitions, ...restAst } = ast
    const operation = this.getOperation(instructions) as OperationTypeNode

    return {
      kind: Kind.DOCUMENT,
      definitions: [
        {
          kind: Kind.OPERATION_DEFINITION,
          operation,
          directives: [],
          variableDefinitions,
          selectionSet: {
            kind: Kind.SELECTION_SET,
            selections: [restAst],
          },
        },
      ],
    }
  }

  processInstructionsOnce = (id: number): Promise<any> => {
    if (!this._promises[id]) {
      this._promises[id] = this.processInstructions(id)
    }

    return this._promises[id]
  }

  processInstructions = async (id: number): Promise<any> => {
    log('process instructions')
    const instructions = this._currentInstructions[id]

    const { variables } = this.generateSelections(instructions)

    const document = this.getDocumentForInstructions(id)
    const operation = this.getOperation(instructions) as OperationTypeNode

    if (this._debug) {
      console.log(`\nQuery:`)
      const query = print(document)
      console.log(query)
      if (variables && Object.keys(variables).length > 0) {
        console.log('Variables:')
        console.log(JSON.stringify(variables))
      }
    }

    log('printed / before')
    const result = await this.execute(operation, document, variables)
    log('executed')

    if (operation === 'subscription') {
      return this.mapSubscriptionPayload(result, instructions)
    }

    return this.extractPayload(result, instructions)
  }

  mapSubscriptionPayload(result, instructions) {
    return mapAsyncIterator(result, res => {
      const extracted = this.extractPayload(res, instructions)
      return extracted
    })
  }

  extractPayload(result, instructions) {
    let pointer = result
    let count = 0
    while (
      pointer &&
      typeof pointer === 'object' &&
      !Array.isArray(pointer) &&
      count < instructions.length
    ) {
      pointer = pointer[Object.keys(pointer)[0]]
      count++
    }
    log('unpack it')

    const lastInstruction = instructions[count - 1]
    const selectionFromFragment = Boolean(lastInstruction.fragment)

    if (
      !selectionFromFragment &&
      Boolean(pointer) &&
      Array.isArray(pointer) &&
      pointer.length > 0
    ) {
      /*
        As per the spec: https://github.com/prisma/prisma/issues/3309
        We need to remove objects of the shape {__typename: <type>} 
        from the output (except when fragment). Checking one element
        is enough, as they will have the same shape.
      */
      if (
        Object.keys(pointer[0]).length === 1 &&
        Object.keys(pointer[0])[0] === '__typename'
      ) {
        pointer = new Array(pointer.length).fill({})
      }
    }

    if (Boolean(pointer) && !selectionFromFragment && !Array.isArray(pointer)) {
      if (
        Object.keys(pointer).length === 1 &&
        Object.keys(pointer)[0] === '__typename'
      ) {
        pointer = {}
      }
    }

    return pointer
  }

  execute(operation, document, variables) {
    const query = print(document)
    if (operation === 'subscription') {
      const subscription = this._subscriptionClient.request({
        query,
        variables,
      })
      return Promise.resolve(observableToAsyncIterable(subscription))
    }
    return this._client.request(query, variables)
  }

  then = async (id, resolve, reject) => {
    let result
    try {
      result = await this.processInstructionsOnce(id)
      this._releaseMemory(id)
      if (typeof resolve === 'function') {
        return resolve(result)
      }
    } catch (e) {
      this._releaseMemory(id)
      if (typeof reject === 'function') {
        return reject(e)
      }
    }
    return result
  }

  catch = async (id, reject) => {
    try {
      return await this.processInstructionsOnce(id)
    } catch (e) {
      this._releaseMemory(id)
      return reject(e)
    }
  }

  _releaseMemory(id) {
    this._currentInstructions[id] = []
    delete this._promises[id]
  }

  generateSelections(instructions) {
    const variableDefinitions: any[] = []
    const variables = {}
    let variableCounter = {}

    const ast = instructions.reduceRight((acc, instruction, index) => {
      let args: any[] = []
      if (instruction.args && Object.keys(instruction.args).length > 0) {
        Object.entries(instruction.args).forEach(([name, value]) => {
          let variableName
          if (typeof variableCounter[name] === 'undefined') {
            variableName = name
            variableCounter[name] = 0
          } else {
            variableCounter[name]++
            variableName = `${name}_${variableCounter[name]}`
          }
          variables[variableName] = value
          const inputArg = instruction.field.args.find(arg => arg.name === name)
          if (!inputArg) {
            throw new Error(
              `Could not find argument ${name} for type ${this.getTypeName(
                instruction.field.type,
              )}`,
            )
          }

          variableDefinitions.push({
            kind: Kind.VARIABLE_DEFINITION,
            variable: {
              kind: Kind.VARIABLE,
              name: {
                kind: Kind.NAME,
                value: variableName,
              },
            },
            type: inputArg.astNode.type,
          })

          args.push({
            kind: Kind.ARGUMENT,
            name: {
              kind: Kind.NAME,
              value: name,
            },
            value: {
              kind: Kind.VARIABLE,
              name: {
                kind: 'Name',
                value: variableName,
              },
            },
          })
        })
      }

      let node: any = {
        kind: Kind.FIELD,
        name: {
          kind: Kind.NAME,
          value: instruction.fieldName,
        },
        arguments: args,
        directives: [],
        selectionSet: {
          kind: Kind.SELECTION_SET,
          selections: [] as any[],
        },
      }

      const type = this.getDeepType(instruction.field.type)
      if (
        index === instructions.length - 1 &&
        type instanceof GraphQLObjectType
      ) {
        if (instruction.fragment) {
          if (typeof instruction.fragment === 'string') {
            instruction.fragment = gql`
              ${instruction.fragment}
            `
          }
          node.selectionSet = node = {
            kind: Kind.FIELD,
            name: {
              kind: Kind.NAME,
              value: instruction.fieldName,
            },
            arguments: args,
            directives: [],
            selectionSet: instruction.fragment.definitions[0].selectionSet,
          }
        } else {
          const rootTypeName = this.getDeepType(instructions[0].field.type).name

          node = this.getFieldAst({
            field: instruction.field,
            fieldName: instruction.fieldName,
            isRelayConnection: this.isConnectionTypeName(rootTypeName),
            isSubscription: instructions[0].typeName === 'Subscription',
            args,
          })
        }
      }

      if (acc) {
        node.selectionSet.selections.push(acc)
      }

      if (
        node.selectionSet.selections.length === 0 &&
        type instanceof GraphQLObjectType
      ) {
        node.selectionSet.selections = [
          {
            kind: 'Field',
            name: { kind: 'Name', value: '__typename' },
            arguments: [],
            directives: [],
          },
        ]
      }

      return node
    }, null)

    return {
      ast: { ...ast, variableDefinitions },
      variables,
    }
  }

  isScalar(field) {
    const fieldType = this.getDeepType(field.type)

    return (
      fieldType instanceof GraphQLScalarType ||
      fieldType instanceof GraphQLEnumType
    )
  }

  isEmbedded(field) {
    const model = this._models.find(m => m.name === field.type.name)
    return model && model.embedded
  }

  isConnectionTypeName(typeName: string) {
    return typeName.endsWith('Connection') && typeName !== 'Connection'
  }

  getFieldAst({ field, fieldName, isRelayConnection, isSubscription, args }) {
    const node: any = {
      kind: Kind.FIELD,
      name: {
        kind: Kind.NAME,
        value: fieldName,
      },
      arguments: args,
      directives: [],
    }

    if (this.isScalar(field)) {
      return node
    }

    node.selectionSet = {
      kind: Kind.SELECTION_SET,
      selections: [] as any[],
    }

    const type = this.getDeepType(field.type)

    if (isRelayConnection) {
      const relayConnectionHasScalars = connectionNodeHasScalars({ type })
      if (this.isConnectionTypeName(fieldName) && !relayConnectionHasScalars) {
        return node
      }
    }

    node.selectionSet.selections = Object.entries(type.getFields())
      .filter(([, subField]: any) => {
        const isScalar = this.isScalar(subField)
        if (isScalar) {
          return true
        }
        const fieldType = this.getDeepType(subField.type)

        if (isRelayConnection) {
          if (subField.name === 'pageInfo' && fieldType.name === 'PageInfo') {
            return true
          }

          if (subField.name === 'edges' && fieldType.name.endsWith('Edge')) {
            return true
          }

          if (
            subField.name === 'node' &&
            fieldName === 'edges' &&
            type.name.endsWith('Edge')
          ) {
            return true
          }

          return false
        }

        if (isSubscription) {
          if (['previousValues', 'node'].includes(subField.name)) {
            return true
          }

          return false
        }

        const model =
          this._models && this._models.find(m => m.name === fieldType.name)
        const embedded = model && model.embedded

        return embedded
      })
      .map(([fieldName, field]: [string, any]) => {
        return this.getFieldAst({
          field,
          fieldName: fieldName,
          isRelayConnection,
          isSubscription,
          args: [],
        })
      })

    return node
  }

  buildMethods() {
    this._types = this.getTypes()
    Object.assign(this, this._types.Query)
    Object.assign(this, this._types.Mutation)
    this.$subscribe = this._types.Subscription
  }

  getTypes() {
    const typeMap = this._schema.getTypeMap()
    const types = Object.entries(typeMap)
      .map(([name, type]) => {
        let value = {
          then: this.then,
          catch: this.catch,
          [Symbol.toStringTag]: 'Promise',
        }
        if (type instanceof GraphQLObjectType) {
          const fieldsArray = (Object.entries(type.getFields()) as any).concat([
            [`$fragment`, null],
          ])
          value = {
            ...value,
            ...fieldsArray
              .map(([fieldName, field]) => {
                return {
                  key: fieldName,
                  value: (args, arg2) => {
                    const id = typeof args === 'number' ? args : ++instructionId

                    let realArgs = typeof args === 'number' ? arg2 : args
                    this._currentInstructions[id] =
                      this._currentInstructions[id] || []

                    if (fieldName === '$fragment') {
                      const currentInstructions = this._currentInstructions[id]
                      currentInstructions[
                        currentInstructions.length - 1
                      ].fragment = arg2
                      return mapValues(value, (key, v) => {
                        if (typeof v === 'function') {
                          return v.bind(this, id)
                        }
                        return v
                      })
                    } else {
                      if (this._currentInstructions[id].length === 0) {
                        if (name === 'Mutation') {
                          if (fieldName.startsWith('create')) {
                            if (Boolean(realArgs)) {
                              realArgs = { data: realArgs }
                            }
                          }
                          if (fieldName.startsWith('delete')) {
                            if (Boolean(realArgs)) {
                              realArgs = { where: realArgs }
                            }
                          }
                        } else if (
                          name === 'Query' ||
                          name === 'Subscription'
                        ) {
                          if (field.args.length === 1) {
                            if (Boolean(realArgs)) {
                              realArgs = { where: realArgs }
                            }
                          }
                        }
                      }
                      this._currentInstructions[id].push({
                        fieldName,
                        args: realArgs,
                        field,
                        typeName: type.name,
                      })
                      const typeName = this.getTypeName(field.type)

                      // this is black magic. what we do here: bind both .then, .catch and all resolvers to `id`
                      return mapValues(this._types[typeName], (key, value) => {
                        if (typeof value === 'function') {
                          return value.bind(this, id)
                        }
                        return value
                      })
                    }
                  },
                }
              })
              .reduce(reduceKeyValue, {}),
          }
        }

        return {
          key: name,
          value,
        }
      })
      .reduce(reduceKeyValue, {})

    return types
  }

  getTypeName(type): string {
    if (type.ofType) {
      return this.getDeepType(type.ofType)
    }
    return type.name
  }

  getDeepType(type) {
    if (type.ofType) {
      return this.getDeepType(type.ofType)
    }

    return type
  }

  private buildGraphQL() {
    return <T = any>(query, variables): Promise<T> => {
      return this._client.request(query, variables)
    }
  }

  private buildExists(): Exists {
    const queryType = this._schema.getQueryType()
    if (!queryType) {
      return {}
    }
    if (queryType) {
      const types = getTypesAndWhere(queryType)

      return types.reduce((acc, { type, pluralFieldName }) => {
        const firstLetterLowercaseTypeName =
          type[0].toLowerCase() + type.slice(1)
        return {
          ...acc,
          [firstLetterLowercaseTypeName]: args => {
            // TODO: when the fragment api is there, only add one field
            return this[pluralFieldName]({ where: args }).then(res => {
              return res.length > 0
            })
          },
        }
      }, {})
    }

    return {}
  }
}

const reduceKeyValue = (acc, curr) => ({
  ...acc,
  [curr.key]: curr.value,
})
