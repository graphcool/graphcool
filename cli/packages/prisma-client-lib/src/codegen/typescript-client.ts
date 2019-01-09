import {
  isNonNullType,
  isListType,
  isScalarType,
  isObjectType,
  isEnumType,
  GraphQLObjectType,
  GraphQLUnionType,
  GraphQLInterfaceType,
  GraphQLInputObjectType,
  GraphQLInputField,
  GraphQLField,
  GraphQLInputType,
  GraphQLOutputType,
  GraphQLWrappingType,
  GraphQLNamedType,
  GraphQLScalarType,
  GraphQLEnumType,
  GraphQLFieldMap,
  GraphQLObjectType as GraphQLObjectTypeRef,
  printSchema,
} from 'graphql'

import { Generator } from './Generator'
import { getExistsTypes } from '../utils'

import * as flatten from 'lodash.flatten'
import * as prettier from 'prettier'
import { codeComment } from '../utils/codeComment'

export interface RenderOptions {
  endpoint?: string
  secret?: string
}

export class TypescriptGenerator extends Generator {
  genericsDelimiter = '='
  lineBreakDelimiter = ''
  partialType = 'Partial'
  prismaInterface = 'Prisma'
  exportPrisma = true
  scalarMapping = {
    Int: 'number',
    String: 'string',
    ID: 'string | number',
    Float: 'number',
    Boolean: 'boolean',
    DateTimeInput: 'Date | string',
    DateTimeOutput: 'string',
    Json: 'any',
  }

  graphqlRenderers = {
    GraphQLUnionType: (type: GraphQLUnionType): string => {
      return `${this.renderDescription(type.description!)}export type ${
        type.name
      } = ${type
        .getTypes()
        .map(t => t.name)
        .join(' | ')}`
    },

    GraphQLObjectType: (
      type:
        | GraphQLObjectTypeRef
        | GraphQLInputObjectType
        | GraphQLInterfaceType,
    ): string => {
      return (
        this.renderInterfaceOrObject(type, true) +
        '\n\n' +
        this.renderInterfaceOrObject(type, false) +
        '\n\n' +
        this.renderInterfaceOrObject(type, false, true)
      )
    },

    GraphQLInterfaceType: (
      type:
        | GraphQLObjectTypeRef
        | GraphQLInputObjectType
        | GraphQLInterfaceType,
    ): string => this.renderInterfaceOrObject(type),

    GraphQLInputObjectType: (
      type:
        | GraphQLObjectTypeRef
        | GraphQLInputObjectType
        | GraphQLInterfaceType,
    ): string => {
      const fieldDefinition = Object.keys(type.getFields())
        .map(f => {
          const field = type.getFields()[f]
          const isOptional = !isNonNullType(field.type)
          return `  ${this.renderFieldName(field, false)}${
            isOptional ? '?' : ''
          }: ${this.renderInputFieldType(field.type)}`
        })
        .join(`${this.lineBreakDelimiter}\n`)

      let interfaces: GraphQLInterfaceType[] = []
      if (type instanceof GraphQLObjectType) {
        interfaces = (type as any).getInterfaces()
      }

      return this.renderInterfaceWrapper(
        type.name,
        type.description!,
        interfaces,
        fieldDefinition,
      )
    },

    GraphQLScalarType: (type: GraphQLScalarType): string => {
      if (type.name === 'ID') {
        return this.graphqlRenderers.GraphQLIDType(type)
      }
      return `${
        type.description
          ? `/*
${type.description}
*/
`
          : ''
      }export type ${type.name} = ${this.scalarMapping[type.name] || 'string'}`
    },

    GraphQLIDType: (type: GraphQLScalarType): string => {
      return `${
        type.description
          ? `/*
${type.description}
*/
`
          : ''
      }export type ${type.name}_Input = ${this.scalarMapping[type.name] ||
        'string'}
export type ${type.name}_Output = string`
    },

    GraphQLEnumType: (type: GraphQLEnumType): string => {
      if (type.name === 'PrismaDatabase') {
        return ``
      }
      return `${this.renderDescription(type.description!)}export type ${
        type.name
      } = ${type
        .getValues()
        .map(e => `  '${e.name}'`)
        .join(' |\n')}`
    },
  }

  format(code: string, options: prettier.Options = {}) {
    return prettier.format(code, {
      ...options,
      parser: 'typescript',
    })
  }
  renderAtLeastOne() {
    return `export type AtLeastOne<T, U = {[K in keyof T]: Pick<T, K> }> = Partial<T> & U[keyof U]`
  }

  renderModels() {
    const models = this.internalTypes
      .map(
        i => `{
    name: '${i.name}',
    embedded: ${i.isEmbedded}
  }`,
      )
      .join(',\n')

    return `export const models: Model[] = [${models}]`
  }

  render(options?: RenderOptions) {
    const queries = this.renderQueries()
    const mutations = this.renderMutations()
    return this.format(this.compile`\
${this.renderImports()}

${this.renderAtLeastOne()}

export interface Exists {\n${this.renderExists()}\n}

export type FragmentableArray<T> = Promise<Array<T>> & Fragmentable

export interface Fragmentable {
  $fragment<T>(fragment: string | DocumentNode): Promise<T>
}

${this.exportPrisma ? 'export' : ''} interface ${this.prismaInterface} {
  $exists: Exists;
  $graphql: <T ${
    this.genericsDelimiter
  } any>(query: string, variables?: {[key: string]: any}) => Promise<T>;

  /**
   * Queries
  */

${queries}${queries.length > 0 ? ';' : ''}

  /**
   * Mutations
  */

${mutations}${mutations.length > 0 ? ';' : ''}


  /**
   * Subscriptions
  */

  $subscribe: Subscription;

}

export interface Subscription {
${this.renderSubscriptions()}
}

${this.renderClientConstructor}

/**
 * Types
*/

${this.renderTypes()}

/**
 * Model Metadata
*/

${this.renderModels()}

/**
 * Type Defs
*/

${this.renderExports(options)}
`)
  }
  renderClientConstructor() {
    return `export interface ClientConstructor<T> {
  new(options?: BaseClientOptions): T
}`
  }
  renderImports() {
    return `\
${codeComment}

import { DocumentNode } from 'graphql'
import { makePrismaClientClass, BaseClientOptions, Model } from 'prisma-client-lib'
import { typeDefs } from './prisma-schema'`
  }
  renderPrismaClassArgs(options?: RenderOptions) {
    let endpointString = ''
    let secretString = ''
    if (options) {
      if (options.endpoint) {
        endpointString = options.endpoint
          ? `, endpoint: ${options.endpoint}`
          : ''
      }
      if (options.secret) {
        secretString = options.secret ? `, secret: ${options.secret}` : ''
      }
    }

    return `{typeDefs, models${endpointString}${secretString}}`
  }
  renderExports(options?: RenderOptions) {
    const args = this.renderPrismaClassArgs(options)
    return `export const Prisma = makePrismaClientClass<ClientConstructor<${
      this.prismaInterface
    }>>(${args})
export const prisma = new Prisma()`
  }
  renderTypedefsFirstLine() {
    return ''
  }
  renderTypedefs() {
    return (
      `${this.renderTypedefsFirstLine()}export const typeDefs = /* GraphQL */ \`` +
      printSchema(this.schema).replace(/`/g, '\\`') +
      '`'
    )
  }
  renderExists() {
    const queryType = this.schema.getQueryType()
    if (queryType) {
      return `${getExistsTypes(queryType)}`
    }
    return ''
  }
  renderQueries() {
    const queryType = this.schema.getQueryType()
    if (!queryType) {
      return ''
    }
    return this.renderMainMethodFields('query', queryType.getFields(), false)
  }
  renderMutations() {
    const mutationType = this.schema.getMutationType()
    if (!mutationType) {
      return ''
    }
    return this.renderMainMethodFields(
      'mutation',
      mutationType.getFields(),
      true,
    )
  }
  renderSubscriptions() {
    const queryType = this.schema.getSubscriptionType()
    if (!queryType) {
      return ''
    }
    return this.renderMainMethodFields(
      'subscription',
      queryType.getFields(),
      false,
    )
  }
  getTypeNames() {
    const ast = this.schema
    // Create types
    return Object.keys(ast.getTypeMap())
      .filter(typeName => !typeName.startsWith('__'))
      .filter(typeName => typeName !== (ast.getQueryType() as any).name)
      .filter(typeName =>
        ast.getMutationType()
          ? typeName !== (ast.getMutationType()! as any).name
          : true,
      )
      .filter(typeName =>
        ast.getSubscriptionType()
          ? typeName !== (ast.getSubscriptionType()! as any).name
          : true,
      )
      .sort((a, b) =>
        (ast.getType(a) as any).constructor.name <
        (ast.getType(b) as any).constructor.name
          ? -1
          : 1,
      )
  }
  renderTypes() {
    const typeNames = this.getTypeNames()
    return flatten(
      typeNames.map(typeName => {
        const forbiddenTypeNames = ['then', 'catch']
        if (forbiddenTypeNames.includes(typeName)) {
          throw new Error(
            `Cannot use ${typeName} as a type name as it is reserved.`,
          )
        }

        const type = this.schema.getTypeMap()[typeName]
        if (typeName === 'DateTime') {
          return [
            this.graphqlRenderers.GraphQLScalarType({
              name: 'DateTimeInput',
              description: 'DateTime scalar input type, allowing Date',
            } as any),
            this.graphqlRenderers.GraphQLScalarType({
              name: 'DateTimeOutput',
              description:
                'DateTime scalar output type, which is always a string',
            } as any),
          ]
        }
        return this.graphqlRenderers[type.constructor.name]
          ? this.graphqlRenderers[type.constructor.name](type)
          : null
      }),
    ).join('\n\n')
  }

  renderArgs(
    field: GraphQLField<any, any>,
    isMutation = false,
    isTopLevel = false,
  ) {
    const { args } = field
    const hasArgs = args.length > 0

    const allOptional = args.reduce((acc, curr) => {
      if (!acc) {
        return false
      }

      return !isNonNullType(curr.type)
    }, true)

    // hard-coded for Prisma ease-of-use
    if (isMutation && field.name.startsWith('create')) {
      return `data${allOptional ? '?' : ''}: ${this.renderInputFieldTypeHelper(
        args[0],
        isMutation,
      )}`
    } else if (
      (isMutation && field.name.startsWith('delete')) || // either it's a delete mutation
      (!isMutation &&
        isTopLevel &&
        args.length === 1 &&
        (isObjectType(field.type) || isObjectType((field.type as any).ofType))) // or a top-level single query
    ) {
      return `where${allOptional ? '?' : ''}: ${this.renderInputFieldTypeHelper(
        args[0],
        isMutation,
      )}`
    }

    return `args${allOptional ? '?' : ''}: {${hasArgs ? ' ' : ''}${args
      .map(
        a =>
          `${this.renderFieldName(a, false)}${
            isNonNullType(a.type) ? '' : '?'
          }: ${this.renderInputFieldTypeHelper(a, isMutation)}`,
      )
      .join(', ')}${args.length > 0 ? ' ' : ''}}`
  }

  renderInputFieldTypeHelper(field, isMutation) {
    return this.renderFieldType({
      field,
      node: false,
      input: true,
      partial: false,
      renderFunction: false,
      isMutation,
      operation: false,
      embedded: false,
    })
  }

  renderMainMethodFields(
    operation: string,
    fields: GraphQLFieldMap<any, any>,
    isMutation = false,
  ): string {
    return Object.keys(fields)
      .filter(f => {
        const field = fields[f]
        return !(field.name === 'executeRaw' && isMutation)
      })
      .map(f => {
        const field = fields[f]
        return `    ${field.name}: (${this.renderArgs(
          field,
          isMutation,
          true,
        )}) => ${this.renderFieldType({
          field,
          node: false,
          input: false,
          partial: false,
          renderFunction: false,
          isMutation,
          isSubscription: operation === 'subscription',
          operation: true,
          embedded: false,
        })}`
      })
      .join(';\n')
  }

  getDeepType(type) {
    if (type.ofType) {
      return this.getDeepType(type.ofType)
    }

    return type
  }

  getInternalTypeName(type) {
    const deepType = this.getDeepType(type)
    const name = String(deepType)
    return name === 'ID' ? 'ID_Output' : name
  }

  getPayloadType(operation: string) {
    if (operation === 'subscription') {
      return `Promise<AsyncIterator<T>>`
    }
    return `Promise<T>`
  }

  isEmbeddedType(
    type: GraphQLScalarType | GraphQLObjectType | GraphQLEnumType,
  ) {
    const internalType = this.internalTypes.find(i => i.name === type.name)
    if (internalType && internalType.isEmbedded) {
      return true
    }

    return false
  }

  renderInterfaceOrObject(
    type: GraphQLObjectTypeRef | GraphQLInputObjectType | GraphQLInterfaceType,
    node = true,
    subscription = false,
  ): string {
    const fields = type.getFields()

    if (node && this.isConnectionType(type)) {
      return this.renderConnectionType(type as GraphQLObjectTypeRef)
    }

    if (node && this.isSubscriptionType(type)) {
      return this.renderSubscriptionType(type as GraphQLObjectTypeRef)
    }

    const fieldDefinition = Object.keys(fields)
      .filter(f => {
        const deepType = this.getDeepType(fields[f].type)
        return node
          ? !isObjectType(deepType) || this.isEmbeddedType(deepType)
          : true
      })
      .map(f => {
        const field = fields[f]
        const deepType = this.getDeepType(fields[f].type)
        const embedded = this.isEmbeddedType(deepType)
        return `  ${this.renderFieldName(field, node)}: ${this.renderFieldType({
          field,
          node,
          input: false,
          partial: false,
          renderFunction: true,
          isMutation: false,
          isSubscription: subscription,
          operation: false,
          embedded,
        })}`
      })
      .join(`${this.lineBreakDelimiter}\n`)

    let interfaces: GraphQLInterfaceType[] = []
    if (type instanceof GraphQLObjectType) {
      interfaces = (type as any).getInterfaces()
    }

    return this.renderInterfaceWrapper(
      type.name,
      type.description!,
      interfaces,
      fieldDefinition,
      !node,
      subscription,
    )
  }

  renderFieldName(
    field: GraphQLInputField | GraphQLField<any, any>,
    node: boolean,
  ) {
    if (!node) {
      return `${field.name}`
    }
    return `${field.name}${isNonNullType(field.type) ? '' : '?'}`
  }

  wrapType(type, subscription = false, isArray = false) {
    if (subscription) {
      return `Promise<AsyncIterator<${type}>>`
    }

    if (isArray) {
      return `FragmentableArray<${type}>`
    }

    return `Promise<${type}>`
  }

  renderFieldType({
    field,
    node,
    input,
    partial,
    renderFunction,
    isMutation = false,
    isSubscription = false,
    operation = false,
    embedded = false,
  }: {
    field
    node: boolean
    input: boolean
    partial: boolean
    renderFunction: boolean
    isMutation: boolean
    isSubscription?: boolean
    operation: boolean
    embedded: boolean
    // node: boolean = true,
    // input: boolean = false,
    // partial: boolean = false,
    // renderFunction: boolean = true,
  }) {
    const { type } = field
    const deepType = this.getDeepType(type)
    const isList = isListType(type) || isListType(type.ofType)
    const isOptional = !(isNonNullType(type) || isNonNullType(type.ofType))
    const isScalar = isScalarType(deepType) || isEnumType(deepType)
    const isInput = field.astNode.kind === 'InputValueDefinition'
    // const isObject = isObjectType(deepType)

    let typeString = this.getInternalTypeName(type)

    if (typeString === 'DateTime') {
      if (isInput) {
        typeString += 'Input'
      } else {
        typeString += 'Output'
      }
    }

    const addSubscription = !partial && isSubscription && !isScalar

    if (
      operation &&
      !node &&
      !isInput &&
      !isList &&
      !isScalar &&
      !addSubscription
    ) {
      return typeString === 'Node' ? `Node` : `${typeString}Promise`
    }

    if ((node || isList) && !isScalar && !addSubscription) {
      typeString += ``
    }

    if (addSubscription) {
      typeString += 'Subscription'
    }

    if (isScalar && !isInput) {
      if (isList) {
        typeString += `[]`
      }
      if (node) {
        return typeString
      } else {
        return `(${
          field.args && field.args.length > 0
            ? this.renderArgs(field, isMutation)
            : ''
        }) => ${this.wrapType(typeString, isSubscription)}`
      }
    }

    if ((isList || node) && isOptional) {
      typeString += ' | null'
    }

    if (isList) {
      if (isScalar) {
        return `${typeString}[]`
      } else {
        if (renderFunction) {
          return `<T ${this.genericsDelimiter} ${this.wrapType(
            `${typeString}`,
            isSubscription,
            true,
          )}> (${
            field.args && field.args.length > 0
              ? this.renderArgs(field, isMutation, false)
              : ''
          }) => T`
        } else {
          return this.wrapType(typeString, isSubscription, true)
        }
      }
    }

    if (partial) {
      typeString = `${this.partialType}<${typeString}>`
    }

    if (embedded && node) {
      return typeString
    }

    if (node && (!isInput || isScalar)) {
      return this.wrapType(`${typeString}`, isSubscription)
    }

    if (isInput || !renderFunction) {
      return typeString
    }

    const promiseString =
      !isList && !isScalar && !isSubscription ? 'Promise' : ''

    return `<T ${this.genericsDelimiter} ${typeString}${promiseString}>(${
      field.args && field.args.length > 0
        ? this.renderArgs(field, isMutation, false)
        : ''
    }) => T`
  }

  renderInputFieldType(type: GraphQLInputType | GraphQLOutputType) {
    if (isNonNullType(type)) {
      return this.renderInputFieldType((type as GraphQLWrappingType).ofType)
    }
    if (isListType(type)) {
      let inputType = this.renderInputFieldType(
        (type as GraphQLWrappingType).ofType,
      )
      if (inputType === 'DateTime') {
        inputType += 'Input'
      }
      return this.renderInputListType(inputType)
    }
    let name = (type as GraphQLNamedType).name
    if (name === 'DateTime') {
      name += 'Input'
    }
    return `${name}${(type as GraphQLNamedType).name === 'ID' ? '_Input' : ''}`
  }

  renderInputListType(type) {
    return `${type}[] | ${type}`
  }

  renderTypeWrapper(
    typeName: string,
    typeDescription: string | void,
    fieldDefinition: string,
  ): string {
    return `${this.renderDescription(
      typeDescription,
    )}export type ${typeName} = {
${fieldDefinition}
}`
  }

  renderInterfaceWrapper(
    typeName: string,
    typeDescription: string | void,
    interfaces: GraphQLInterfaceType[],
    fieldDefinition: string,
    promise?: boolean,
    subscription?: boolean,
  ): string {
    const actualInterfaces = promise
      ? [
          {
            name: subscription
              ? `Promise<AsyncIterator<${typeName}>>`
              : `Promise<${typeName}>`,
          },
          {
            name: 'Fragmentable',
          },
          // Filter out 'Node' since promise or subscription interface
          // shouldn't extend it directly
        ].concat(interfaces.filter(i => i.name !== 'Node'))
      : interfaces

    return `${this.renderDescription(typeDescription)}${
      // TODO: Find a better solution than the hacky replace to remove ? from inside AtLeastOne
      typeName.includes('WhereUniqueInput')
        ? `export type ${typeName} = AtLeastOne<{
        ${fieldDefinition.replace('?:', ':')}
      }>`
        : `export interface ${typeName}${
            promise && !subscription ? 'Promise' : ''
          }${subscription ? 'Subscription' : ''}${
            actualInterfaces.length > 0
              ? ` extends ${actualInterfaces.map(i => i.name).join(', ')}`
              : ''
          } {
      ${fieldDefinition}
      }`
    }`
  }

  renderDescription(description?: string | void) {
    return `${
      description
        ? `/*
${description.split('\n').map(l => ` * ${l}\n`)}
 */
`
        : ''
    }`
  }

  isConnectionType(
    type: GraphQLObjectTypeRef | GraphQLInputObjectType | GraphQLInterfaceType,
  ) {
    if (!(type instanceof GraphQLObjectTypeRef)) {
      return false
    }

    const fields = type.getFields()

    if (type.name.endsWith('Connection') && type.name !== 'Connection') {
      return !Object.keys(fields).some(
        f => ['pageInfo', 'aggregate', 'edges'].includes(f) === false,
      )
    }

    if (type.name.endsWith('Edge') && type.name !== 'Edge') {
      return !Object.keys(fields).some(
        f => ['cursor', 'node'].includes(f) === false,
      )
    }

    return false
  }

  renderConnectionType(type: GraphQLObjectTypeRef) {
    const fields = type.getFields()
    let fieldDefinition: string[] = []
    const connectionFieldsType = {
      pageInfo: fieldType => fieldType,
      edges: fieldType => `${fieldType}[]`,
    }

    if (type.name.endsWith('Connection')) {
      fieldDefinition = Object.keys(fields)
        .filter(f => f !== 'aggregate')
        .map(f => {
          const field = fields[f]
          const deepType = this.getDeepType(fields[f].type)

          return `  ${this.renderFieldName(
            field,
            false,
          )}: ${connectionFieldsType[field.name](deepType.name)}`
        })
    } else {
      // else if type.name is typeEdge
      fieldDefinition = Object.keys(fields).map(f => {
        const field = fields[f]
        const deepType = this.getDeepType(fields[f].type)

        return `  ${this.renderFieldName(field, false)}: ${deepType.name}`
      })
    }

    return this.renderInterfaceWrapper(
      `${type.name}`,
      type.description!,
      [],
      fieldDefinition.join(`${this.lineBreakDelimiter}\n`),
      false,
      false,
    )
  }

  isSubscriptionType(
    type: GraphQLObjectType | GraphQLInputObjectType | GraphQLInterfaceType,
  ) {
    if (!(type instanceof GraphQLObjectTypeRef)) {
      return false
    }

    const fields = type.getFields()

    if (
      type.name.endsWith('SubscriptionPayload') &&
      type.name !== 'SubscriptionPayload'
    ) {
      return !Object.keys(fields).some(
        f =>
          ['mutation', 'node', 'updatedFields', 'previousValues'].includes(
            f,
          ) === false,
      )
    }

    return false
  }

  private renderSubscriptionType(type: GraphQLObjectType) {
    const fields = type.getFields()

    const fieldsType = {
      mutation: fieldType => fieldType,
      node: fieldType => fieldType,
      previousValues: fieldType => fieldType,
      updatedFields: fieldType => `${fieldType}[]`,
    }

    const fieldDefinition = Object.keys(fields)
      .map(f => {
        const field = fields[f]
        const deepType = this.getDeepType(fields[f].type)

        return `  ${this.renderFieldName(field, false)}: ${fieldsType[
          field.name
        ](deepType.name)}`
      })
      .join(`${this.lineBreakDelimiter}\n`)

    return this.renderInterfaceWrapper(
      `${type.name}`,
      type.description!,
      [],
      fieldDefinition,
      false,
      false,
    )
  }
}
