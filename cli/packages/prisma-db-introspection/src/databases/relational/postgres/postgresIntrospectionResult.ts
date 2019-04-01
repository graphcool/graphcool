import {
  ITable,
  ITableRelation,
  IEnum,
  ISequenceInfo,
} from '../relationalConnector'
import { RelationalIntrospectionResult } from '../relationalIntrospectionResult'
import {
  IGQLField,
  IGQLType,
  DatabaseType,
  Renderer,
  TypeIdentifiers,
  GQLAssert,
  IdStrategy,
} from 'prisma-datamodel'

export class PostgresIntrospectionResult extends RelationalIntrospectionResult {
  constructor(
    model: ITable[],
    relations: ITableRelation[],
    enums: IEnum[],
    sequences: ISequenceInfo[],
    renderer?: Renderer,
  ) {
    super(model, relations, enums, sequences, DatabaseType.postgres, renderer)
  }

  protected isTypeReserved(type: IGQLType): boolean {
    return type.name == '_RelayId'
  }
  protected toTypeIdentifyer(
    fieldTypeName: string,
    fieldInfo: IGQLField,
    typeName: string,
  ): string | null {
    switch (fieldTypeName) {
      case 'int1':
      case 'int2':
      case 'int4':
      case '_int4':
      case 'int8':
        return TypeIdentifiers.integer
      case 'numeric':
      case 'float4':
      case 'float8':
        return TypeIdentifiers.float
      case 'varchar':
      case 'bpchar':
      case '_text':
      // If we have a text type on an ID field, we map to the ID type.
      case 'text':
        return fieldInfo.isId ? TypeIdentifiers.id : TypeIdentifiers.string
      case 'bool':
        return TypeIdentifiers.boolean
      case 'jsonb':
      case 'json':
        return TypeIdentifiers.json
      case '_date':
      case 'date':
      case 'timestamptz':
      case 'timestamp':
        return TypeIdentifiers.dateTime
      case 'uuid':
        return TypeIdentifiers.uuid
      default:
        return null
    }
  }
  protected parseDefaultValue(
    defaultValueString: string,
    type: string,
  ): string | null {
    let val = defaultValueString

    // Remove cast operator
    const i = val.indexOf('::')

    if (i >= 0) {
      val = val.substring(0, i)
    }

    // Detect string
    if (val.startsWith("'")) {
      // Strip quotes (are added again by renderer)
      val = val.substring(1, val.length - 1)
    }

    // Check for null
    if (val.toUpperCase() === 'NULL') {
      return null
    }

    // If the field is not a string field,
    // and the default val is not a boolean or a number, we assume a function call or sequence reference.
    if (type !== TypeIdentifiers.string && type != TypeIdentifiers.id) {
      if (
        isNaN(val as any) &&
        val.toLowerCase() !== 'true' &&
        val.toLowerCase() !== 'false'
      ) {
        return null
      }
    }

    // TODO: Sequences are simply ignored.

    return val
  }

  protected resolveSequences(types: IGQLType[], sequences: ISequenceInfo[]) {
    for (const type of types) {
      for (const field of type.fields) {
        if (
          typeof field.defaultValue === 'string' &&
          field.defaultValue.startsWith('nextval')
        ) {
          // Regex also truncates the database schema name, if included in the regex. that's the first capture group.
          const match = field.defaultValue.match(
            /^nextval\('(?:.*\.)?(.*?)'::regclass\)$/i,
          )

          if (match === null) {
            continue
          }
          let [dummy, seqName] = match

          // Trim quotes.
          if (seqName.startsWith('"')) {
            seqName = seqName.substring(1, seqName.length - 1)
          }

          const seq = sequences.find(x => x.name === seqName)

          if (seq === undefined) {
            field.comments.push({
              text: `Error resolving sequence ${seqName} for ${type.name}.${
                field.name
              }: The sequence was not found.`,
              isError: true,
            })
          } else {
            field.idStrategy = IdStrategy.Sequence
            field.associatedSequence = seq!
            field.defaultValue = null
          }
        }
      }
    }
    return types
  }
}
