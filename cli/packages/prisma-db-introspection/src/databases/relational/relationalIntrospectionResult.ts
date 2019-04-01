import { IntrospectionResult } from '../../common/introspectionResult'
import {
  ITable,
  IColumn,
  IIndex,
  ITableRelation,
  IEnum,
  ISequenceInfo,
} from './relationalConnector'
import {
  ISDL,
  DatabaseType,
  Renderer,
  IGQLField,
  IGQLType,
  TypeIdentifier,
  IIndexInfo,
  GQLAssert,
  IComment,
  camelCase,
  TypeIdentifiers,
  IdStrategy,
  LegacyRelationalReservedFields,
} from 'prisma-datamodel'

export abstract class RelationalIntrospectionResult extends IntrospectionResult {
  protected model: ITable[]
  protected relations: ITableRelation[]
  protected enums: IEnum[]
  protected sequences: ISequenceInfo[]

  constructor(
    model: ITable[],
    relations: ITableRelation[],
    enums: IEnum[],
    sequences: ISequenceInfo[],
    databaseType: DatabaseType,
    renderer?: Renderer,
  ) {
    super(databaseType, renderer)

    this.model = model
    this.relations = relations
    this.enums = enums
    this.sequences = sequences
  }

  /**
   * @deprecated This returns an unnormalized datamodel and might get removed in the near future.
   */
  public getDatamodel(): ISDL {
    return this.infer(this.model, this.enums, this.relations, this.sequences)
  }

  protected resolveRelations(types: IGQLType[], relations: ITableRelation[]) {
    for (const relation of relations) {
      this.resolveRelation(types, relation)
    }
    return types
  }

  private getDatabaseName(obj: IGQLType | IGQLField) {
    if (obj.databaseName !== null) {
      return obj.databaseName
    } else {
      return obj.name
    }
  }

  protected normalizeRelatioName(name: string) {
    if (name.startsWith('_')) {
      return name.substring(1) // This is most likely a prisma relation name
    } else {
      return name
    }
  }

  protected resolveEnumTypes(types: IGQLType[]) {
    for (const enumType of types) {
      if (!enumType.isEnum) continue

      for (const type of types) {
        for (const field of type.fields) {
          if (typeof field.type === 'string') {
            if (field.type === enumType.name) {
              // Remove type error hint and set enum type
              field.comments = field.comments.filter(
                comment =>
                  comment.text !== `Type ${field.type} is not supported`,
              )
              field.type = enumType
            }
          }
        }
      }
    }
    return types
  }

  protected abstract resolveSequences(
    types: IGQLType[],
    sequences: ISequenceInfo[],
  ): IGQLType[]

  protected resolveRelation(types: IGQLType[], relation: ITableRelation) {
    // Correctly sets field types according to given FK constraints.
    for (const typeA of types) {
      for (const typeB of types) {
        for (const fieldA of typeA.fields) {
          for (const fieldB of typeB.fields) {
            if (
              relation.sourceColumn === this.getDatabaseName(fieldA) &&
              relation.sourceTable === this.getDatabaseName(typeA) &&
              relation.targetColumn === this.getDatabaseName(fieldB) &&
              relation.targetTable === this.getDatabaseName(typeB)
            ) {
              if (!fieldB.isId) {
                fieldA.comments.push({
                  text: `Relation ${typeA.name}.${fieldA.name} -> ${
                    typeB.name
                  }.${fieldB.name} does not target the id field of ${
                    typeB.name
                  }`,
                  isError: true,
                })
              }

              fieldA.type = typeB

              // Add back connecting
              // TODO: We could look at the data to see if this is 1:1 or 1:n. For now, we use a unique constraint. Tell Tim.
              const connectorFieldAtB: IGQLField = {
                // TODO - how do we name that field?
                // Problems:
                // * Conflicts
                // * Need to identify field in existing datamodel to fix naming, fix cardinality or hide
                name: camelCase(typeA.name),
                databaseName: null,
                defaultValue: null,
                isList: !fieldA.isUnique,
                isCreatedAt: false,
                isUpdatedAt: false,
                isId: false,
                idStrategy: null,
                associatedSequence: null,
                isReadOnly: false,
                isRequired: fieldA.isRequired, // TODO: Not sure if that makes sense
                isUnique: false,
                relatedField: fieldA,
                type: typeA,
                relationName: null,
                comments: [],
                directives: [],
              }

              // Hook up connector fields
              fieldA.relatedField = connectorFieldAtB
              typeB.fields.push(connectorFieldAtB)
              return
            }
          }
        }
      }
    }

    GQLAssert.raise(
      `Failed to resolve FK constraint ${relation.sourceTable}.${
        relation.sourceColumn
      } -> ${relation.targetTable}.${relation.targetColumn}`,
    )
  }
  protected markNonIdFieldsWithSequencesAsErrored(
    types: IGQLType[],
  ): IGQLType[] {
    for (const type of types) {
      for (const field of type.fields) {
        if (field.idStrategy === IdStrategy.Sequence && !field.isId) {
          field.comments.push({
            text: 'Only id fields can have sequences',
            isError: true,
          })
        }
      }
    }
    return types
  }
  protected markInvalidIndexes(types: IGQLType[]) {
    for (const type of types) {
      for (const indexInfo of type.indices) {
        const errorFields = indexInfo.fields.filter(field =>
          field.comments.some(comment => comment.isError),
        )

        if (errorFields.length > 0) {
          indexInfo.comments.push({
            text: `This index is invalid, since the following fields have errors: ${errorFields
              .map(field => field.name)
              .join('\n')}`,
            isError: true,
          })
        }
      }
    }
    return types
  }

  protected markMultiIdFieldsForJoinTabesAsErrors(
    types: IGQLType[],
  ): IGQLType[] {
    for (const type of types) {
      if (!type.isLinkTable) {
        const pkFields = type.fields.filter(field => field.isId)
        if (pkFields.length > 1) {
          for (const field of pkFields) {
            field.comments.push({
              text: `Multiple ID fields (compound indexes) are not supported`,
              isError: true,
            })
          }
        }
      }
    }
    return types
  }
  protected resolveFallbackIdField(types: IGQLType[]): IGQLType[] {
    for (const type of types) {
      const idField = type.fields.find(x => x.isId)

      if (idField === undefined) {
        // Okay, we find alternate indices.

        // First, is there a single field with a sequence or auto increment?
        const fieldsWithSequences = type.fields.filter(
          field =>
            field.idStrategy === IdStrategy.Auto ||
            field.idStrategy === IdStrategy.Sequence,
        )

        if (fieldsWithSequences.length === 1) {
          fieldsWithSequences[0].isId = true
          continue
        }

        // If not, is there something called id?
        const idFields = type.fields.filter(
          field =>
            field.name === LegacyRelationalReservedFields.idFieldName &&
            field.isUnique,
        )

        if (idFields.length === 1) {
          idFields[0].isId = true
          continue
        }

        // If not, is there a single unique field?
        const uniqueField = type.fields.filter(field => field.isUnique)

        if (uniqueField.length === 1) {
          uniqueField[0].isId = true
          continue
        }
      }
    }
    return types
  }

  protected hideScalarListTypes(types: IGQLType[]): IGQLType[] {
    const scalarListTypes: IGQLType[] = []

    for (const type of types) {
      for (const field of type.fields) {
        if (typeof field.type === 'string') continue

        const candidate = field.type
        // A type is only a scalar list iff it has
        // * name of ${type.name}_${field.name}
        // * Has exactly three fields
        //     * nodeId: typeof type!
        //     * position: Int!
        //     * value: ?
        // TODO: Tim mentioned, but not observed in the wild.
        // * compound index over nodeId and position

        if (candidate.fields.length !== 3) continue

        if (candidate.name === `${type.name}_${field.name}`) continue

        const [nodeId] = candidate.fields.filter(
          field =>
            field.name === 'nodeId' &&
            field.type === type &&
            field.isRequired == true &&
            field.isList === false,
        )

        const [position] = candidate.fields.filter(
          field =>
            field.name === 'position' &&
            field.type === TypeIdentifiers.integer &&
            field.isRequired == true &&
            field.isList === false,
        )

        const [value] = candidate.fields.filter(
          field =>
            field.name === 'value' &&
            field.isRequired == true &&
            field.isList === false,
        )

        if (
          nodeId === undefined ||
          position === undefined ||
          value === undefined
        )
          continue

        // If we got so far, we have found a scalar list type. Hurray!
        scalarListTypes.push(candidate)

        // Update the field to show a scalar list
        field.type = value.type
        field.isList = true

        // Update the name, if it follows prisma conventions
        // e.g. user_scalarIntList => scalarIntList
        if (field.name.startsWith(`${camelCase(type.name)}_`)) {
          field.name = field.name.substring(type.name.length + 1)
        }
      }
    }

    // Filter out scalar list types
    return types.filter(type => !scalarListTypes.includes(type))
  }

  /**
   * Removes all types which are only there for NM relations
   * @param types
   */
  protected hideJoinTypes(types: IGQLType[]) {
    const nonJoinTypes: IGQLType[] = []

    for (const type of types) {
      const relationFields = type.fields.filter(
        field => typeof field.type !== 'string',
      )
      const relationAndIdFields = type.fields.filter(
        field => typeof field.type !== 'string' || field.isId,
      )

      // A join type only as two relation fields. And sometimes a primary key.
      const isJoinType =
        (relationFields.length === 2 && type.fields.length === 2) ||
        (relationAndIdFields.length === 3 && type.fields.length === 3)

      if (isJoinType) {
        // We drop the join type, but add a n:m relation to
        // the two related tables.
        const [relA, relB] = relationFields

        // Type checking was done above
        const typeA = relA.type as IGQLType
        const typeB = relB.type as IGQLType

        if (
          (relA.name === 'A' && relB.name === 'B') ||
          (relB.name === 'A' && relA.name === 'B')
        ) {
          // In this case, this is a prisma link table. Hide it.
          if (true || typeA !== typeB) {
            // Regular case. Two different types via join type.
            const relatedFieldForA: IGQLField = {
              name: typeB.name,
              type: typeB,
              isList: true,
              isUnique: false,
              isId: false,
              idStrategy: null,
              associatedSequence: null,
              isCreatedAt: false,
              isUpdatedAt: false,
              isRequired: true,
              isReadOnly: false,
              comments: [],
              directives: [],
              defaultValue: null,
              relatedField: null,
              databaseName: null,
              relationName: this.normalizeRelatioName(type.name),
            }

            const relatedFieldForB: IGQLField = {
              name: typeA.name,
              type: typeA,
              isList: true,
              isUnique: false,
              isId: false,
              idStrategy: null,
              associatedSequence: null,
              isCreatedAt: false,
              isUpdatedAt: false,
              isRequired: true,
              isReadOnly: false,
              comments: [],
              directives: [],
              defaultValue: null,
              relatedField: relatedFieldForA,
              databaseName: null,
              relationName: this.normalizeRelatioName(type.name),
            }

            relatedFieldForA.relatedField = relatedFieldForB

            typeA.fields.push(relatedFieldForA)
            typeB.fields.push(relatedFieldForB)
          } else {
            // Self join to same field via join type.
            const relatedField: IGQLField = {
              name: typeA.name,
              type: typeA,
              isList: true,
              isUnique: false,
              isId: false,
              idStrategy: null,
              associatedSequence: null,
              isCreatedAt: false,
              isUpdatedAt: false,
              isRequired: true,
              isReadOnly: false,
              comments: [],
              directives: [],
              defaultValue: null,
              relatedField: null,
              databaseName: null,
              relationName: this.normalizeRelatioName(type.name),
            }

            typeA.fields.push(relatedField)
          }
          typeA.fields = typeA.fields.filter(x => x.type !== type)
          typeB.fields = typeB.fields.filter(x => x.type !== type)
        } else {
          // Not a prisma link type. Mark as link table.
          type.isLinkTable = true
          // Drop ids. Compound PK indices are not supported yet.
          relA.isId = false
          relB.isId = false
          nonJoinTypes.push(type)
        }
      } else {
        nonJoinTypes.push(type)
      }
    }

    return nonJoinTypes
  }

  /**
   * Hides indices on related fields. These are always autmatically created by
   * prisma and dont need to be shown in the datamodel.
   * @param types
   */
  protected hideIndicesOnRelatedFields(types: IGQLType[]) {
    for (const type of types) {
      // Keep indices which have not exactly one field, or if they have one field,
      // the field is a scalar field.
      type.indices = type.indices.filter(
        index =>
          index.fields.length !== 1 || typeof index.fields[0].type === 'string',
      )
    }
    return types
  }

  /**
   * Hides unique inidices and marks the corresponding fields as unique instead.
   * @param types
   */
  protected hideUniqueIndices(types: IGQLType[]) {
    for (const type of types) {
      const uniqueIndices = type.indices.filter(
        index => index.fields.length === 1 && index.unique,
      )

      for (const uniqueIndex of uniqueIndices) {
        uniqueIndex.fields[0].isUnique = true
        type.indices = type.indices.filter(x => x !== uniqueIndex)
      }
    }
    return types
  }

  protected abstract isTypeReserved(type: IGQLType): boolean

  protected hideReservedTypes(types: IGQLType[]) {
    return types.filter(x => !this.isTypeReserved(x))
  }

  protected infer(
    model: ITable[],
    enums: IEnum[],
    relations: ITableRelation[],
    sequences: ISequenceInfo[],
  ): ISDL {
    // TODO: Maybe we want to have a concept of hidden, which just skips rendering?
    // Ask tim, this is an important descision for the SDK
    let types = [
      ...model.map(x => this.inferObjectType(x)),
      ...enums.map(x => this.inferEnumType(x)),
    ]
    types = this.hideUniqueIndices(types)
    types = this.resolveSequences(types, sequences)
    types = this.resolveFallbackIdField(types) // unique flags and index types are required for this step.
    types = this.inferDefaultValues(types)
    types = this.resolveRelations(types, relations)
    types = this.resolveEnumTypes(types)
    types = this.hideJoinTypes(types)
    types = this.hideReservedTypes(types)
    types = this.hideScalarListTypes(types)
    types = this.hideIndicesOnRelatedFields(types)
    types = this.hideJoinTypes(types)
    types = this.markNonIdFieldsWithSequencesAsErrored(types)
    types = this.markMultiIdFieldsForJoinTabesAsErrors(types)
    types = this.markInvalidIndexes(types)

    return {
      comments: [],
      types: types,
    }
  }

  protected inferIndex(index: IIndex, fields: IGQLField[]): IIndexInfo {
    const fieldCandidates = fields.filter(
      field =>
        index.fields.filter(indexField => field.name === indexField).length > 0,
    )

    const indexInfo: IIndexInfo = {
      fields: fieldCandidates,
      name: index.name,
      unique: index.unique,
      comments: [],
    }

    return indexInfo
  }

  // We need info about indices for resolving the exact type, as String is mapped to ID.
  // Also, we need info about the actual type before we resolve default values.
  protected inferDefaultValues(types: IGQLType[]) {
    for (const type of types) {
      for (const field of type.fields) {
        this.inferFieldTypeAndDefaultValue(field, type.name)
      }
    }
    return types
  }

  protected inferEnumType(model: IEnum): IGQLType {
    const values = model.values.map(
      (x: string): IGQLField => ({
        name: x,
        isCreatedAt: false,
        isId: false,
        idStrategy: null,
        associatedSequence: null,
        isList: false,
        isReadOnly: false,
        isRequired: false,
        isUnique: false,
        isUpdatedAt: false,
        relatedField: null,
        relationName: null,
        type: TypeIdentifiers.string,
        defaultValue: null,
        databaseName: null,
        directives: [],
        comments: [],
      }),
    )

    return {
      name: model.name,
      fields: values,
      isEnum: true,
      isEmbedded: false,
      isLinkTable: false,
      databaseName: null,
      comments: [],
      directives: [],
      indices: [],
    }
  }

  protected inferObjectType(model: ITable): IGQLType {
    const fields = model.columns.map(x => this.inferField(x))
    const indices = model.indices.map(x => this.inferIndex(x, fields))

    // Resolve primary key
    if (model.primaryKey !== null) {
      const pk = model.primaryKey
      if (pk.fields.length === 1) {
        // Single PK field - prisma can do that
        const [pkField] = fields.filter(field => field.name === pk.fields[0])
        if (!pkField) {
          GQLAssert.raise(
            `Index/Schema missmatch during introspection. Field ${
              pk.fields[0]
            } used in index, but does not exist on table ${model.name}`,
          )
        }
        pkField.isId = true
      } else {
        // Compound PK - that's not supported, except for join tables
        // We will mark it as error later.
        for (const pkFieldName of pk.fields) {
          const [pkField] = fields.filter(field => field.name === pkFieldName)
          if (!pkField) {
            GQLAssert.raise(
              `Index/Schema missmatch during introspection. Field ${
                pk.fields[0]
              } used in index, but does not exist on table ${model.name}`,
            )
          }
          pkField.isId = true
        }
      }
    }

    return {
      name: model.name,
      isEmbedded: false, // Never
      isEnum: false, // Never
      isLinkTable: false, // Resolved Later
      fields,
      indices,
      directives: [],
      comments: [],
      databaseName: null,
    }
  }

  protected inferField(field: IColumn): IGQLField {
    const comments: IComment[] = []

    if (field.comment !== null) {
      comments.push({
        text: field.comment,
        isError: false,
      })
    }

    const gqlField: IGQLField = {
      name: field.name,
      isUnique: field.isUnique,
      isRequired: !field.isNullable,
      defaultValue: field.defaultValue,
      isList: field.isList,
      type: field.type,
      isId: false, // Will resolve later, from indices
      idStrategy: field.isAutoIncrement ? IdStrategy.Auto : IdStrategy.None,
      associatedSequence: null,
      relatedField: null,
      relationName: null,
      isCreatedAt:
        LegacyRelationalReservedFields.createdAtFieldName === field.name, // Heuristic, can be overriden by normalization
      isUpdatedAt:
        LegacyRelationalReservedFields.updatedAtFieldName === field.name, // Heuristic, can be overriden by normalization
      isReadOnly: false, // Never
      comments,
      directives: [],
      databaseName: null,
    }

    return gqlField
  }

  protected inferFieldTypeAndDefaultValue(field: IGQLField, typeName: string) {
    GQLAssert.raiseIf(
      typeof field.type !== 'string',
      'Must be called before resolving relations',
    )
    let type: string | null = this.toTypeIdentifyer(
      field.type as string,
      field,
      typeName,
    )

    if (type === null) {
      field.comments.push({
        text: `Type ${field.type} is not supported`,
        isError: true,
      })
      // Keep native type and register an error.
    } else {
      field.type = type
    }

    if (field.defaultValue !== null) {
      GQLAssert.raiseIf(
        typeof field.defaultValue !== 'string',
        'Must be called with unparsed default values.',
      )
      field.defaultValue = this.parseDefaultValue(
        field.defaultValue as string,
        field.type as string,
      )
    }
  }

  /**
   * Maps a native database type. If null is returned, the corresponding
   * field is marked with an error comment.
   * @param typeName
   */
  protected abstract toTypeIdentifyer(
    fieldTypeName: string,
    fieldInfo: IGQLField,
    typeName: string,
  ): string | null

  protected abstract parseDefaultValue(
    defaultValueString: string,
    type: string,
  ): string | null
}
