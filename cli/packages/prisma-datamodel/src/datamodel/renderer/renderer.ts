import {
  ISDL,
  IGQLType,
  IDirectiveInfo,
  IGQLField,
  IIndexInfo,
  IdStrategy,
} from '../model'
import { GraphQLSchema } from 'graphql/type/schema'
import {
  GraphQLObjectType,
  GraphQLEnumType,
  GraphQLField,
  GraphQLFieldConfig,
} from 'graphql/type/definition'
import { GraphQLDirective } from 'graphql/type/directives'
import { DirectiveKeys } from '../directives'
import { TypeIdentifiers } from '../scalar'

const indent = '  '
const comment = '#'

export default abstract class Renderer {
  // We keep optional sorting support because
  // it increases testability of this class.
  private sortBeforeRendering: boolean

  public render(input: ISDL, sortBeforeRendering: boolean = false): string {
    this.sortBeforeRendering = sortBeforeRendering
    // Sort alphabetically. Enums last.
    const sortedTypes = this.sortTypes(input.types)

    return sortedTypes
      .map(t => {
        if (t.isEnum) {
          return this.renderEnum(t)
        } else {
          return this.renderType(t)
        }
      })
      .join(`\n\n`)
  }

  protected createIsEmbeddedTypeDirective(type: IGQLType) {
    return { name: DirectiveKeys.isEmbedded, arguments: {} }
  }

  protected createIsLinkTableTypeDirective(type: IGQLType) {
    return { name: DirectiveKeys.linkTable, arguments: {} }
  }

  protected createDatabaseNameTypeDirective(type: IGQLType) {
    return {
      name: DirectiveKeys.db,
      arguments: {
        name: this.renderValue(TypeIdentifiers.string, type.databaseName),
      },
    }
  }

  protected createIndexDirectives(
    type: IGQLType,
    typeDirectives: IDirectiveInfo[],
  ) {
    if (type.indices.length > 0) {
      const indexDescriptions: string[] = []
      for (const index of type.indices) {
        indexDescriptions.push(this.createIndexDirective(index))
      }
      typeDirectives.push({
        name: DirectiveKeys.indexes,
        arguments: {
          value: `[\n${indexDescriptions.join(',\n')}\n]`,
        },
      })
    }
  }

  protected createIndexDirective(index: IIndexInfo) {
    const directive: IDirectiveInfo = {
      name: DirectiveKeys.index,
      arguments: {
        name: this.renderValue(TypeIdentifiers.string, index.name),
        // Special rendering: We escape manually here to render an array.
        fields: `[${index.fields
          .map(x => this.renderValue(TypeIdentifiers.string, x.name))
          .join(', ')}]`,
      },
    }

    if (index.unique) {
      directive.arguments = {
        ...directive.arguments,
        unique: this.renderValue(TypeIdentifiers.boolean, index.unique),
      }
    }

    const { renderedComments, hasError } = this.renderComments(index, indent)
    const commentPrefix = hasError ? `${comment} ` : ''
    const renderedArguments = Object.keys(directive.arguments)
      .map(x => `${x}: ${directive.arguments[x]}`)
      .join(', ')

    if (renderedComments.length > 0) {
      return `${renderedComments}\n${indent}${commentPrefix}{${renderedArguments}}`
    } else {
      return `${indent}{${renderedArguments}}`
    }
  }

  protected shouldCreateIsEmbeddedTypeDirective(type: IGQLType) {
    return type.isEmbedded
  }

  protected shouldCreateIsLinkTableTypeDirective(type: IGQLType) {
    return type.isLinkTable
  }

  protected shouldCreateDatabaseNameTypeDirective(type: IGQLType) {
    return type.databaseName && !type.isEmbedded
  }

  protected shouldRenderIndexDirectives(type: IGQLType) {
    return type.indices.length > 0
  }

  protected createReservedTypeDirectives(
    type: IGQLType,
    typeDirectives: IDirectiveInfo[],
  ) {
    if (this.shouldCreateIsEmbeddedTypeDirective(type)) {
      typeDirectives.push(this.createIsEmbeddedTypeDirective(type))
    }
    if (this.shouldCreateDatabaseNameTypeDirective(type)) {
      typeDirectives.push(this.createDatabaseNameTypeDirective(type))
    }
    if (this.shouldCreateIsLinkTableTypeDirective(type)) {
      typeDirectives.push(this.createIsLinkTableTypeDirective(type))
    }
    if (this.shouldRenderIndexDirectives(type)) {
      this.createIndexDirectives(type, typeDirectives)
    }
  }

  protected renderType(type: IGQLType): string {
    const typeDirectives: IDirectiveInfo[] = [...type.directives]

    this.createReservedTypeDirectives(type, typeDirectives)

    const renderedDirectives = this.renderDirectives(typeDirectives)
    const sortedFields = this.sortFields(type.fields)
    const renderedFields = sortedFields.map(x => this.renderField(x))

    const renderedTypeName =
      renderedDirectives.length > 0
        ? `type ${type.name} ${renderedDirectives}`
        : `type ${type.name}`

    const { renderedComments, hasError } = this.renderComments(type, '')
    const allFieldsHaveError = type.fields.every(x =>
      x.comments.some(c => c.isError),
    )

    const commentPrefix = allFieldsHaveError ? `${comment} ` : ''

    if (renderedComments.length > 0) {
      return `${renderedComments}\n${commentPrefix}${renderedTypeName} {\n${renderedFields.join(
        '\n',
      )}\n${commentPrefix}}`
    } else {
      return `${commentPrefix}${renderedTypeName} {\n${renderedFields.join(
        '\n',
      )}\n${commentPrefix}}`
    }
  }

  protected renderComments(
    type: IGQLType | IGQLField | IIndexInfo,
    spacing: string,
  ) {
    const renderedComments = type.comments
      .map(x => `${spacing}${comment} ${x.text}`)
      .join('\n')
    const hasError = type.comments.some(x => x.isError)

    return { renderedComments, hasError }
  }

  protected createDefaultValueFieldDirective(field: IGQLField) {
    return {
      name: DirectiveKeys.default,
      arguments: { value: this.renderValue(field.type, field.defaultValue) },
    }
  }

  protected createIsUniqueFieldDirective(field: IGQLField) {
    return { name: DirectiveKeys.isUnique, arguments: {} }
  }

  protected createRelationNameFieldDirective(field: IGQLField) {
    return {
      name: DirectiveKeys.relation,
      arguments: {
        name: this.renderValue(TypeIdentifiers.string, field.relationName),
      },
    }
  }

  protected createIsIdFieldDirective(field: IGQLField) {
    const args = {} as any

    if (field.idStrategy !== null && field.idStrategy !== IdStrategy.Auto) {
      args.strategy = field.idStrategy
    }

    return { name: DirectiveKeys.isId, arguments: args }
  }

  protected createSequenceFieldDirective(field: IGQLField): IDirectiveInfo {
    const sequence = field.associatedSequence!
    return {
      name: DirectiveKeys.sequence,
      arguments: {
        name: this.renderValue(TypeIdentifiers.string, sequence.name),
        initialValue: this.renderValue(
          TypeIdentifiers.integer,
          sequence.initialValue,
        ),
        allocationSize: this.renderValue(
          TypeIdentifiers.integer,
          sequence.allocationSize,
        ),
      },
    }
  }
  protected createIsCreatedAtFieldDirective(field: IGQLField) {
    return { name: DirectiveKeys.isCreatedAt, arguments: {} }
  }

  protected createIsUpdatedAtFieldDirctive(field: IGQLField) {
    return { name: DirectiveKeys.isUpdatedAt, arguments: {} }
  }

  protected createDatabaseNameFieldDirective(field: IGQLField) {
    return {
      name: DirectiveKeys.db,
      arguments: {
        name: this.renderValue(TypeIdentifiers.string, field.databaseName),
      },
    }
  }

  protected shouldCreateDefaultValueFieldDirective(field: IGQLField) {
    return field.defaultValue !== null
  }

  protected shouldCreateIsUniqueFieldDirective(field: IGQLField) {
    return field.isUnique && !field.isId
  }

  protected shouldCreateRelationNameFieldDirective(field: IGQLField) {
    return field.relationName !== null
  }

  protected shouldCreateIsIdFieldDirective(field: IGQLField) {
    return field.isId
  }

  protected shouldCreateSequenceFieldDirective(field: IGQLField) {
    return field.associatedSequence !== null
  }

  protected shouldCreateCreatedAtFieldDirective(field: IGQLField) {
    return field.isCreatedAt
  }

  protected shouldCreateUpdatedAtFieldDirective(field: IGQLField) {
    return field.isUpdatedAt
  }

  protected shouldCreateDatabaseNameFieldDirective(field: IGQLField) {
    return field.databaseName !== null
  }

  protected createReservedFieldDirectives(
    field: IGQLField,
    fieldDirectives: IDirectiveInfo[],
  ) {
    if (this.shouldCreateDefaultValueFieldDirective(field)) {
      fieldDirectives.push(this.createDefaultValueFieldDirective(field))
    }
    if (this.shouldCreateIsUniqueFieldDirective(field)) {
      fieldDirectives.push(this.createIsUniqueFieldDirective(field))
    }
    if (this.shouldCreateRelationNameFieldDirective(field)) {
      fieldDirectives.push(this.createRelationNameFieldDirective(field))
    }
    if (this.shouldCreateIsIdFieldDirective(field)) {
      fieldDirectives.push(this.createIsIdFieldDirective(field))
    }
    if (this.shouldCreateSequenceFieldDirective(field)) {
      fieldDirectives.push(this.createSequenceFieldDirective(field))
    }
    if (this.shouldCreateCreatedAtFieldDirective(field)) {
      fieldDirectives.push(this.createIsCreatedAtFieldDirective(field))
    }
    if (this.shouldCreateUpdatedAtFieldDirective(field)) {
      fieldDirectives.push(this.createIsUpdatedAtFieldDirctive(field))
    }
    if (this.shouldCreateDatabaseNameFieldDirective(field)) {
      fieldDirectives.push(this.createDatabaseNameFieldDirective(field))
    }
  }

  protected renderField(field: IGQLField): string {
    const fieldDirectives: IDirectiveInfo[] = [...field.directives]

    this.createReservedFieldDirectives(field, fieldDirectives)

    const renderedDirectives = this.renderDirectives(fieldDirectives)

    let type = this.extractTypeIdentifier(field.type)
    if (field.isList) {
      // Lists are always required in Prisma
      type = `[${type}]`
    } else if (field.isRequired) {
      type = `${type}!`
    }

    const renderedField =
      renderedDirectives.length > 0
        ? `${field.name}: ${type} ${renderedDirectives}`
        : `${field.name}: ${type}`

    const { renderedComments, hasError } = this.renderComments(field, indent)

    if (renderedComments.length > 0) {
      if (hasError) {
        return `${renderedComments}\n${indent}${comment} ${renderedField}`
      } else {
        return `${renderedComments}\n${indent}${renderedField}`
      }
    } else {
      return `${indent}${renderedField}`
    }
  }

  protected renderEnum(type: IGQLType): string {
    const values: string[] = []

    for (const field of type.fields) {
      if (field.defaultValue !== null) {
        values.push(
          `${indent}${field.name} = ${this.renderValue(
            field.type as string,
            field.defaultValue,
          )}`,
        )
      } else {
        values.push(`${indent}${field.name}`)
      }
    }

    return `enum ${type.name} {\n${values.join('\n')}\n}`
  }

  protected renderDirectives(directives: IDirectiveInfo[]): string {
    const sortedDirectives = [...directives].sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1,
    )
    return this.mergeDirectives(sortedDirectives)
      .map(x => this.renderDirective(x))
      .join(` `)
  }

  protected renderDirective(directive: IDirectiveInfo): string {
    const args: string[] = []

    for (const arg of Object.keys(directive.arguments)) {
      // TODO: We don't know the type. Is this a problem?
      args.push(`${arg}: ${this.renderValue('', directive.arguments[arg])}`)
    }

    if (args.length > 0) {
      return `@${directive.name}(${args.join(', ')})`
    } else {
      return `@${directive.name}`
    }
  }

  /**
   * Merges directives by summarizing arguments of
   * directives with equal name. That saves work when adding directives.
   */
  protected mergeDirectives(directives: IDirectiveInfo[]): IDirectiveInfo[] {
    // Group by name
    const grouped = directives.reduce((r, a) => {
      r[a.name] = r[a.name] || []
      r[a.name].push(a)
      return r
    }, {})

    const merged: IDirectiveInfo[] = []

    // Merge with same name
    for (const name of Object.keys(grouped)) {
      if (name === DirectiveKeys.index) {
        // Do not summarize index directives
        for (const directive of grouped[name]) {
          merged.push(directive)
        }
      } else {
        merged.push({
          name,
          arguments: grouped[name].reduce((r, a) => {
            return { ...a.arguments, ...r.arguments }
          }, {}),
        })
      }
    }

    return merged
  }

  protected extractTypeIdentifier(type: string | IGQLType) {
    if (typeof type === 'string') {
      return type
    } else {
      return type.name
    }
  }

  protected renderValue(type: string | IGQLType, value: any) {
    const strType = this.extractTypeIdentifier(type)
    if (
      strType === TypeIdentifiers.string ||
      strType === TypeIdentifiers.json ||
      strType === TypeIdentifiers.id ||
      strType === TypeIdentifiers.dateTime
    ) {
      return `"${value}"`
    } else {
      return value
    }
  }

  protected sortTypes(types: IGQLType[]) {
    if (!this.sortBeforeRendering) {
      return types
    } else {
      return [...types].sort((a, b) => {
        if (a.isEnum === b.isEnum) {
          return a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1
        } else if (b.isEnum) {
          return -1
        } else {
          return 1
        }
      })
    }
  }

  protected sortFields(fields: IGQLField[]) {
    if (!this.sortBeforeRendering) {
      return fields
    } else {
      return [...fields].sort((a, b) =>
        a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1,
      )
    }
  }
}
