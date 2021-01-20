import {
  GraphQLScalarType,
  GraphQLNonNull,
  GraphQLList,
  GraphQLObjectType,
  GraphQLType,
} from 'graphql'
import {
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLBoolean,
  GraphQLID,
} from 'graphql/type'

// tslint:disable:max-classes-per-file

/**
 * Represents a list of name, value pairs
 * to represent arguments.
 */
export interface IArguments {
  readonly [name: string]: string
}

/**
 * Represents a directive
 */
export interface IDirectiveInfo {
  name: string
  arguments: IArguments
}

/**
 * Represents a comment.
 * If the error flag is set, the comment indicates an error
 * and should be treated accordingly.
 */
export interface IComment {
  text: string
  isError: boolean
}

export interface IIndexInfo {
  name: string
  fields: IGQLField[]
  unique: boolean
}

export enum IdStrategy {
  Auto = 'AUTO',
  None = 'NONE',
  Sequence = 'SEQUENCE',
}

export interface ISequenceInfo {
  name: string
  initialValue: number
  allocationSize: number
}

/**
 * Represents a field in the datamodel.
 */
export interface IGQLField {
  /**
   * The name of this field.
   */
  name: string
  /**
   * The type of this field. A value of
   * type string indicates a scalar type.
   */
  type: string | IGQLType
  /**
   * Indicates if this field is required.
   */
  isRequired: boolean
  /**
   * Indicates if this field holds
   * a list of values. Can be set for
   * scalars or relations.
   */
  isList: boolean
  /**
   * The default value of a given field,
   * if any.
   */
  defaultValue: string | number | null
  /**
   * This is set for double-sided relations.
   */
  relatedField: IGQLField | null
  /**
   * This is only set for relations with a name given via
   * an directive.
   */
  relationName: string | null
  /**
   * Indicates if this field is unique across the model.
   */
  isUnique: boolean

  /**
   * Indicates if this field is the unique identifier.
   */
  isId: boolean

  /**
   * Indicates the id strategy to use. Null defaults to Auto.
   */
  idStrategy: IdStrategy | null

  /**
   * Indicates if a sequence is associated with this field.
   * Only valid if the field is an ID field with idType set to Sequence.
   */
  associatedSequence: ISequenceInfo | null

  /**
   * Indicates if this field is the created at timestamp.
   */
  isCreatedAt: boolean

  /**
   * Indicates if this field is the updated at timestamp.
   */
  isUpdatedAt: boolean

  /**
   * Indicates if this field is read-only.
   */
  isReadOnly: boolean

  /**
   * Indicates how this field is called in the database. If this value is not set,
   * the name in the database is equal to the field name.
   */
  databaseName: string | null

  /**
   * Indicates this fields extra directives,
   * which can not expressed using this
   * interface's other members.
   */
  directives: IDirectiveInfo[]

  /**
   * Comments for this field.
   */
  comments: IComment[]
}

/**
 * Represents a type in the datamodel.
 */
export interface IGQLType {
  /**
   * Indicates if this is an embedded type.
   */
  isEmbedded: boolean
  /**
   * Indicates if this is an enum type.
   */
  isEnum: boolean

  /**
   * Indicates if this type is a link table.
   * This implies a compound primary key over
   * the two foreign key fields. The indices are hidden
   * when introspecting.
   */
  isRelationTable: boolean

  /**
   * The name of this type.
   */
  name: string
  /**
   * A list of all fields of this type.
   *
   * If this is an enum type, only the name properties of each
   * field are relevant.
   */
  fields: IGQLField[]

  /**
   * Indicates how this type is called in the database. If this value is not set,
   * the name in the database is equal to the type name.
   *
   * This field is ignored for embedded types, which never have a database name.
   */
  databaseName: string | null

  /**
   * Indicates this types extra directives,
   * which can not expressed using this
   * interface's other members.
   */
  directives: IDirectiveInfo[]

  /**
   * Comments for this type.
   */
  comments: IComment[]

  /**
   * Indices for this type.
   *
   * Will be parsed and rendered to the corresponding directive.
   */
  indices: IIndexInfo[]
}

export interface ISDL {
  /**
   * All types in this datamodel.
   */
  types: IGQLType[]

  /**
   * Comments for this datamodel.
   */
  comments?: IComment[]
}

/**
 * Internal manifestations of the interfaces declared above.
 */
export class GQLFieldBase implements IGQLField {
  public name: string
  public type: string | IGQLType
  public isRequired: boolean
  public isList: boolean
  public relatedField: IGQLField | null
  public relationName: string | null
  public isUnique: boolean
  public defaultValue: any
  public isCreatedAt: boolean
  public isUpdatedAt: boolean
  public isId: boolean
  public idStrategy: IdStrategy | null
  public associatedSequence: ISequenceInfo | null
  public isReadOnly: boolean
  public databaseName: string | null
  public directives: IDirectiveInfo[]
  public comments: IComment[]

  constructor(name: string, type: IGQLType | string, isRequired?: boolean) {
    this.name = name
    this.type = type
    this.isRequired = isRequired || false
    this.isList = false
    this.relatedField = null
    this.relationName = null
    this.isUnique = false
    this.defaultValue = null
    this.isCreatedAt = false
    this.isUpdatedAt = false
    this.isId = false
    this.idStrategy = null
    this.associatedSequence = null
    this.isReadOnly = false
    this.databaseName = null
    this.directives = []
    this.comments = []
  }
}

export class GQLScalarField extends GQLFieldBase {
  constructor(name: string, type: string | IGQLType, isRequired?: boolean) {
    super(name, type, isRequired)
  }
}
export class GQLOneRelationField extends GQLFieldBase {
  constructor(name: string, type: IGQLType, isRequired?: boolean) {
    super(name, type, isRequired)
  }
}

// TODO: Create abstract base class for testing.
export class GQLMultiRelationField extends GQLFieldBase {
  constructor(name: string, type: IGQLType, isRequired?: boolean) {
    super(name, type, isRequired)
    this.isList = true
  }
}

function cloneComments(
  copy: ISDL | IGQLField | IGQLType,
  obj: ISDL | IGQLField | IGQLType,
) {
  if (obj.comments !== undefined) {
    copy.comments = []
    for (const comment of obj.comments) {
      copy.comments.push({ ...comment })
    }
  }
}

function cloneSequence(copy: IGQLField, obj: IGQLField) {
  if (obj.associatedSequence !== null) {
    copy.associatedSequence = { ...obj.associatedSequence }
  }
}

function cloneCommentsAndDirectives(
  copy: IGQLField | IGQLType,
  obj: IGQLField | IGQLType,
) {
  if (obj.directives !== undefined) {
    copy.directives = []
    for (const directive of obj.directives) {
      copy.directives.push({ ...directive })
    }
  }

  cloneComments(copy, obj)
}

// 21st of Dec: Start: 8:00 - end: 9:45
export function cloneField(field: IGQLField): IGQLField {
  const copy = {
    ...field,
  }

  cloneCommentsAndDirectives(copy, field)
  cloneSequence(copy, field)

  return copy
}

export function cloneType(type: IGQLType): IGQLType {
  const copy = {
    ...type,
  }

  cloneCommentsAndDirectives(copy, type)
  cloneIndices(copy, type)

  copy.fields = []
  for (const field of type.fields) {
    copy.fields.push(cloneField(field))
  }

  return copy
}

export function cloneIndices(copy: IGQLType, obj: IGQLType) {
  if (obj.indices !== undefined) {
    copy.indices = []

    for (const index of obj.indices) {
      copy.indices.push({
        name: index.name,
        unique: index.unique,
        fields: [...index.fields],
      })
    }
  }
}

/**
 * Deep-copies a datamodel and re-connects all types correctly.
 * @param schema The datamodel to clone.
 */
export function cloneSchema(schema: ISDL): ISDL {
  // TODO(ejoebstl): It would be better to have a concrete implementation for
  // each SDL object and require a clone method on interface level.
  const copy = {
    ...schema,
  }

  cloneComments(copy, schema)

  copy.types = []
  for (const type of schema.types) {
    copy.types.push(cloneType(type))
  }

  // Re-Assign type pointer for relations
  for (const type of copy.types) {
    for (const field of type.fields) {
      if (typeof field.type !== 'string') {
        const typeName = field.type.name
        const [fieldType] = copy.types.filter(x => x.name === typeName)
        console.assert(fieldType !== undefined) // This case should never happen
        field.type = fieldType
      }
    }
  }

  // Re-Assign field pointer for indices
  for (const type of copy.types) {
    if (type.indices !== undefined) {
      for (const index of type.indices) {
        // We need an index for setting the element
        // tslint:disable-next-line:prefer-for-of
        for (let i = 0; i < index.fields.length; i++) {
          const fieldName = index.fields[i].name
          const [field] = type.fields.filter(x => x.name === fieldName)
          console.assert(field !== undefined) // This case should never happen
          index.fields[i] = field
        }
      }
    }
  }

  return copy
}
