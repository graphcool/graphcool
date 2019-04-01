import {
  IGQLType,
  IGQLField,
  GQLScalarField,
  ISDL,
  IDirectiveInfo,
  IArguments,
  IIndexInfo,
  IdStrategy,
  ISequenceInfo,
} from '../model'
import { parse } from 'graphql'
import { DirectiveKeys } from '../directives'
import GQLAssert from '../../util/gqlAssert'

// TODO(ejoebstl): It would be good to have this Parser fill the directive field for types and models as well.
// TODO(ejoebstl): Directive parsing should be cleaned up: Parse all directives first and then extract build-in directives.

/**
 * Parses a datamodel given as DSL
 * to an internal representation, convenient for
 * working with.
 */
export default abstract class DefaultParser {
  /**
   * Shorthand to parse the datamodel, given an SDL string.
   * @param schemaString The datamodel as SDL string.
   * @returns A list of types found in the datamodel.
   */
  public parseFromSchemaString(schemaString: string) {
    const schema = parse(schemaString)
    return this.parseFromSchema(schema)
  }

  /**
   * Parses the datamodel from a graphql-js schema.
   * @param schema The graphql-js schema, representing the datamodel.
   * @returns A list of types found in the datamodel.
   */
  public parseFromSchema(schema: any): ISDL {
    const types = [...this.parseTypes(schema)]

    this.resolveRelations(types)

    // That's it.
    // We could check our model here, if we wanted to.
    // Possible checks:
    // * Check if we still use strings for identifying types for non-scalar types
    // * Check if all double-sided relations are connected correctly
    // * Check for duplicate type names
    // * Check for conflicting relations
    return {
      types,
    }
  }

  /**
   * Checks if the given field is an ID field
   * @param field
   */
  protected abstract isIdField(field: any): boolean

  /**
   * Checks if the given field is an updatedAt field
   * @param field
   */
  protected abstract isUpdatedAtField(field: any): boolean

  /**
   * Checks if the given field is a createdAt field
   * @param field
   */
  protected abstract isCreatedAtField(field: any): boolean

  protected parseIdType(field: any) {
    const idDirective = this.getDirectiveByName(field, DirectiveKeys.isId)
    const idType = this.getDirectiveArgument(idDirective, 'strategy')

    if (idType) {
      switch (idType) {
        case IdStrategy.Auto:
          return IdStrategy.Auto
        case IdStrategy.None:
          return IdStrategy.None
        case IdStrategy.Sequence:
          return IdStrategy.Sequence
        default:
          GQLAssert.raise(`Found invalid ID strategy while parsing: ${idType}`)
          return null
      }
    } else {
      return null
    }
  }

  protected parseSequence(field: any): ISequenceInfo | null {
    const sequenceDirective = this.getDirectiveByName(
      field,
      DirectiveKeys.sequence,
    )

    if (sequenceDirective === null) {
      return null
    }

    const name = this.getDirectiveArgument(sequenceDirective, 'name')
    const initialValue = this.getDirectiveArgument(
      sequenceDirective,
      'initialValue',
    )
    const allocationSize = this.getDirectiveArgument(
      sequenceDirective,
      'allocationSize',
    )

    GQLAssert.raiseIf(name === null, 'Name is required in sequence directive.')
    GQLAssert.raiseIf(
      initialValue === null,
      'initialValue is required in sequence directive.',
    )
    GQLAssert.raiseIf(
      allocationSize === null,
      'allocationSize is required in sequence directive.',
    )

    return {
      name: name as string,
      initialValue: parseInt(initialValue, 10),
      allocationSize: parseInt(allocationSize, 10),
    }
  }

  /**
   * Checks if the given field is reserved and read-only.
   * @param field
   */
  protected isReservedReadOnlyField(field: any) {
    return (
      this.isIdField(field) ||
      this.isUpdatedAtField(field) ||
      this.isCreatedAtField(field)
    )
  }

  /**
   * Finds a directives on a field or type by name.
   * @param fieldOrType
   * @param name
   */
  protected getDirectivesByName(fieldOrType: any, name: string): any[] {
    return fieldOrType.directives.filter(x => x.name.value === name)
  }

  /**
   * Finds a directive on a field or type by name and returns the first occurance.
   * @param fieldOrType
   * @param name
   */
  protected getDirectiveByName(fieldOrType: any, name: string): any {
    const directives = this.getDirectivesByName(fieldOrType, name)
    return directives.length > 0 ? directives[0] : null
  }

  /**
   * Checks if a directive on a given field or type ecists
   * @param fieldOrType
   * @param name
   */
  protected hasDirective(fieldOrType: any, name: string): boolean {
    return this.getDirectiveByName(fieldOrType, name) != null
  }

  /**
   * Checks if the given field is unique.
   * @param field
   */
  protected isUniqe(field: any): boolean {
    return this.hasDirective(field, DirectiveKeys.isUnique)
  }

  /**
   * Gets a fields default value. If no default
   * value is given, returns null.
   * @param field
   */
  protected getDefaultValue(field: any): any {
    const directive = this.getDirectiveByName(field, DirectiveKeys.default)
    const args =
      directive === null
        ? []
        : directive.arguments.filter(x => x.name.value === 'value')
    return args.length !== 0 ? args[0].value.value : null
  }

  /**
   * Gets a fields relation name. If no relation
   * exists, returns null.
   * @param field
   */
  protected getRelationName(field: any): string | null {
    const directive = this.getDirectiveByName(field, DirectiveKeys.relation)
    return this.getDirectiveArgument(directive, 'name')
  }

  /**
   * Gets a fields or types relation name. If no directive
   * exists, returns null.
   * @param field
   */
  protected getDatabaseName(fieldOrType: any): string | null {
    const directive = this.getDirectiveByName(fieldOrType, DirectiveKeys.db)
    return this.getDirectiveArgument(directive, 'name')
  }

  /**
   * Gets a fields relation name. If no directive
   * exists, returns null.
   * @param field
   */
  protected getDatabaseFieldName(field: IGQLField): string | null {
    return this.getDatabaseName(field)
  }

  /**
   * Gets a types relation name. If no directive
   * exists, returns null.
   * @param field
   */

  protected getDatabaseTypeName(type: IGQLType): string | null {
    return this.getDatabaseName(type)
  }

  /**
   * Returns the value of a directive argument.
   */
  protected getDirectiveArgument(directive: any, name: string) {
    if (directive && directive.arguments) {
      const nameArgument = directive.arguments.find(a => a.name.value === name)
      if (nameArgument) {
        // Fallback from single value to list value.
        return nameArgument.value.value !== undefined
          ? nameArgument.value.value
          : nameArgument.value.values
      }
    }

    return null
  }

  /**
   * Returns the value of an object field.
   */
  protected getObjectFieldValue(obj: any, name: string) {
    if (obj && obj.fields) {
      const nameArgument = obj.fields.find(a => a.name.value === name)
      if (nameArgument) {
        // Fallback from single value to list value.
        return nameArgument.value.value !== undefined
          ? nameArgument.value.value
          : nameArgument.value.values
      }
    }

    return null
  }

  /**
   * Parses a single index directive input object, resolves all field references.
   */
  protected parseIndex(indexObject: any, fields: IGQLField[]): IIndexInfo {
    const fieldsArgument = this.getObjectFieldValue(indexObject, 'fields')
    const nameArgument = this.getObjectFieldValue(indexObject, 'name')
    const uniqueArgument = this.getObjectFieldValue(indexObject, 'unique')

    const indexFields = fieldsArgument.map(fieldArgument => {
      const [field] = fields.filter(f => f.name === fieldArgument.value)

      if (field === undefined) {
        GQLAssert.raise(
          `Error during index association. Field ${
            fieldArgument.value
          } is missing on index ${nameArgument}.`,
        )
      }

      return field
    })

    return {
      fields: indexFields,
      name: nameArgument,
      // Unique default is true.
      unique: uniqueArgument === null ? true : uniqueArgument,
      comments: [],
    }
  }

  /**
   * Parses all index directives on the given type.
   */
  protected parseIndices(type: any, fields: IGQLField[]): IIndexInfo[] {
    const indexDirective = this.getDirectiveByName(type, DirectiveKeys.indexes)
    if (indexDirective === null) {
      return []
    }
    const subIndexes = this.getDirectiveArgument(indexDirective, 'value')

    return subIndexes.map(directive => this.parseIndex(directive, fields))
  }

  /**
   * Gets all reserved directive keys.
   */
  protected getReservedDirectiveNames() {
    return [
      DirectiveKeys.default,
      DirectiveKeys.isEmbedded,
      DirectiveKeys.db,
      DirectiveKeys.isCreatedAt,
      DirectiveKeys.isUpdatedAt,
      DirectiveKeys.isUnique,
      DirectiveKeys.isId,
      DirectiveKeys.index,
      DirectiveKeys.relation,
      DirectiveKeys.sequence,
    ]
  }

  /**
   * Parses all directives that are not reserved (build-in)
   * from a field or type
   */
  protected parseDirectives(fieldOrType: any) {
    const res: IDirectiveInfo[] = []
    const reservedDirectiveNames = this.getReservedDirectiveNames()

    for (const directive of fieldOrType.directives) {
      if (reservedDirectiveNames.includes(directive.name.value)) {
        continue
      }

      const resArgs = {}
      for (const args of directive.arguments) {
        resArgs[args.name.value] = args.value.value
      }
      res.push({
        name: directive.name.value,
        arguments: resArgs,
      })
    }

    if (res.length === 0) {
      return []
    } else {
      return res
    }
  }

  /**
   * Parses a model field, respects all
   * known directives.
   * @param field
   */
  protected parseField(field: any): IGQLField {
    const name = field.name.value

    const kind = this.parseKind(field.type, null)
    const fieldType = this.parseType(field.type)
    const isId = this.isIdField(field)
    const isUnique = isId || this.isUniqe(field)
    const isReadOnly = this.isReservedReadOnlyField(field)
    const isUpdatedAt = this.isUpdatedAtField(field)
    const isCreatedAt = this.isCreatedAtField(field)
    const defaultValue = this.getDefaultValue(field)
    const relationName = this.getRelationName(field)
    const databaseName = this.getDatabaseFieldName(field)
    const directives = this.parseDirectives(field)
    const associatedSequence = this.parseSequence(field)
    const idStrategy = this.parseIdType(field)

    return {
      name,
      type: fieldType,
      relationName,
      defaultValue,
      isUnique,
      isList: kind === 'ListType',
      isRequired: kind === 'NonNullType',
      relatedField: null,
      isId,
      idStrategy,
      associatedSequence,
      isUpdatedAt,
      isCreatedAt,
      isReadOnly,
      databaseName,
      directives,
      comments: [],
    }
  }

  /**
   * Checks if the given type is an embedded type.
   * @param type
   */
  protected abstract isEmbedded(type: any): boolean

  /**
   * Checks if the given type is marked as link table.
   * @param type
   */
  protected isLinkTable(type: any): boolean {
    return this.hasDirective(type, DirectiveKeys.linkTable)
  }

  /**
   * Parases an object type.
   * @param type
   */
  protected parseObjectType(type: any): IGQLType {
    const fields: IGQLField[] = []
    for (const field of type.fields) {
      if (field.kind === 'FieldDefinition') {
        // Check for type, kind, name and directives.
        fields.push(this.parseField(field))
      }
    }

    const databaseName = this.getDatabaseTypeName(type)
    const isEmbedded = this.isEmbedded(type)
    const isLinkTable = this.isLinkTable(type)
    const directives = this.parseDirectives(type)
    const indices = this.parseIndices(type, fields)

    return {
      name: type.name.value,
      fields,
      isEnum: false,
      isLinkTable,
      isEmbedded,
      databaseName,
      directives,
      indices,
      comments: [],
    }
  }

  /**
   * Parses all types in the schema.
   * @param schema
   */
  protected parseTypes(schema: any): IGQLType[] {
    const types: IGQLType[] = []

    for (const type of schema.definitions) {
      if (type.kind === 'ObjectTypeDefinition') {
        types.push(this.parseObjectType(type))
      } else if (type.kind === 'EnumTypeDefinition') {
        types.push(this.parseEnumType(type))
      }
    }

    return types
  }

  /**
   * Parses an enum type.
   * @param schema
   */
  protected parseEnumType(type: any): IGQLType {
    if (type.kind === 'EnumTypeDefinition') {
      const values: IGQLField[] = []
      for (const value of type.values) {
        if (value.kind === 'EnumValueDefinition') {
          const name = value.name.value

          // All props except name are ignored for enum defs.
          values.push(new GQLScalarField(name, 'String', false))
        }
      }

      const directives = this.parseDirectives(type)

      return {
        name: type.name.value,
        fields: values,
        isEnum: true,
        isLinkTable: false,
        isEmbedded: false,
        directives,
        comments: [],
        databaseName: null,
        indices: [],
      }
    } else {
      throw GQLAssert.raise('Expected an enum type.')
    }
  }

  /**
   * Resolves and connects all realtion fields found
   * in the given type list.
   * @param types
   */
  protected resolveRelations(types: IGQLType[]) {
    // Find all types that we know,
    // and assign a proper type object instead
    // of the string.
    for (const typeA of types) {
      for (const fieldA of typeA.fields) {
        for (const typeB of types) {
          // At this stage, every type is a string
          if ((fieldA.type as string) === typeB.name) {
            fieldA.type = typeB
          }
        }
      }
    }

    // Connect all relations that are named.
    for (const typeA of types) {
      for (const fieldA of typeA.fields) {
        if (typeof fieldA.type === 'string') {
          continue // Assume scalar
        }

        if (fieldA.relationName !== null && fieldA.relatedField === null) {
          for (const fieldB of fieldA.type.fields) {
            if (fieldB.relationName === fieldA.relationName) {
              if (fieldB.type !== typeA) {
                GQLAssert.raise(
                  `Relation type mismatch for relation ${fieldA.relationName}`,
                )
              }
              fieldA.relatedField = fieldB
              fieldB.relatedField = fieldA
              break
            }
          }
        }
      }
    }

    // Connect  obvious relations which are lacking the relatioName directive.
    // We explicitely DO NOT ignore fields with a given relationName, in accordance
    // to the prisma implementation.
    for (const typeA of types) {
      searchThroughAFields: for (const fieldA of typeA.fields) {
        if (typeof fieldA.type === 'string') {
          continue // Assume scalar.
        }
        if (fieldA.relatedField !== null) {
          continue // Nothing to do, already connected
        }

        for (const fieldA2 of typeA.fields) {
          if (fieldA2 !== fieldA && fieldA2.type === fieldA.type) {
            // Skip, A has more than one fields of this relation type.
            continue searchThroughAFields
          }
        }

        const relationPairs: Array<{ a: IGQLField; b: IGQLField }> = []

        // Look for the opposite field by type.
        for (const fieldB of fieldA.type.fields) {
          if (fieldB.type === typeA) {
            if (fieldB !== fieldA) {
              // Don't connect self-referencing fields
              relationPairs.push({ a: fieldA, b: fieldB })
            }
          }
        }

        // Create relation iff we have found a single pair
        if (relationPairs.length === 1) {
          const [{ a, b }] = relationPairs
          a.relatedField = b
          b.relatedField = a
        }
      }
    }
  }

  /**
   * Traverses an AST branch and finds the next type.
   * This will skip modifiers like NonNullType or ListType.
   * @param type
   */
  protected parseType(type: any) {
    if (type.type) {
      return this.parseType(type.type)
    } else if (type.kind !== 'NamedType') {
      throw new Error()
    }
    return type.name.value
  }

  /**
   * Traverses an AST branch and returns the modifier
   * of the type: Either ListType or NonNullType.
   * @param type
   * @param acc
   */
  protected parseKind(type: any, acc: any) {
    if (!acc) {
      acc = type.kind
    }

    // If we find list, we always take list
    if (type.kind === 'ListType') {
      return type.kind
    }

    // Non-null has higher prio than nullable
    if (type.kind === 'NonNullType') {
      acc = type.kind
    }

    // When we reach the end, return whatever we have stored.
    if (type.type) {
      return this.parseKind(type.type, acc)
    } else {
      return acc
    }
  }
}
