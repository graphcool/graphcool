import {
  IGQLType,
  IGQLField,
  GQLScalarField,
  IDirectiveInfo,
  GQLOneRelationField,
  IdStrategy,
} from '../../src/datamodel/model'
import Renderer from '../../src/datamodel/renderer'
import Parser from '../../src/datamodel/parser'
import { DatabaseType } from '../../src/databaseType'
import { dedent } from '../../src/util/util'

describe(`Renderer directives test`, () => {
  test('Render built-in directives correctly with mongo renderer', () => {
    const renderer = Renderer.create(DatabaseType.mongo)

    const modelWithDirectives = dedent(`
      type Test @embedded {
        hasBeenCreatedAt: DateTime @createdAt
        hasBeenUpdatedAt: DateTime @updatedAt
        mappedField: String @db(name: "dbField") @relation(name: "typeRelation")
        primaryId: Int @id
      }`)

    const field1 = new GQLScalarField('hasBeenCreatedAt', 'DateTime')
    field1.isCreatedAt = true
    const field2 = new GQLScalarField('hasBeenUpdatedAt', 'DateTime')
    field2.isUpdatedAt = true
    const field3 = new GQLScalarField('primaryId', 'Int')
    field3.isId = true
    const field4 = new GQLScalarField('mappedField', 'String')
    field4.databaseName = 'dbField'
    field4.relationName = 'typeRelation'

    const type: IGQLType = {
      name: 'Test',
      isEmbedded: true,
      isLinkTable: false,
      // This will be ignored since we are dealing with an embedded type
      databaseName: 'testType',
      isEnum: false,
      fields: [field1, field2, field3, field4],
      directives: [],
      comments: [],
      indices: [],
    }

    const res = renderer.render(
      {
        types: [type],
      },
      true,
    )

    expect(res).toEqual(modelWithDirectives)
  })

  test('Render built-in directives correctly with postgres renderer', () => {
    const renderer = Renderer.create(DatabaseType.postgres)

    const modelWithDirectives = dedent(`
      type Test @pgTable(name: "testType") {
        createdAt: DateTime
        id: Int @unique
        mappedField: String @pgColumn(name: "dbField") @relation(name: "typeRelation")
        updatedAt: DateTime
      }`)

    const field1 = new GQLScalarField('createdAt', 'DateTime')
    field1.isCreatedAt = true
    const field2 = new GQLScalarField('updatedAt', 'DateTime')
    field2.isUpdatedAt = true
    const field3 = new GQLScalarField('id', 'Int')
    field3.isId = true
    const field4 = new GQLScalarField('mappedField', 'String')
    field4.databaseName = 'dbField'
    field4.relationName = 'typeRelation'

    const type: IGQLType = {
      name: 'Test',
      isEmbedded: false,
      isLinkTable: false,
      databaseName: 'testType',
      isEnum: false,
      fields: [field1, field2, field3, field4],
      directives: [],
      comments: [],
      indices: [],
    }

    const res = renderer.render(
      {
        types: [type],
      },
      true,
    )

    expect(res).toEqual(modelWithDirectives)
  })

  test('Render built-in index directive correctly', () => {
    const renderer = Renderer.create(DatabaseType.mongo)

    const modelWithDirectives = dedent(`
      type User @indexes(value: [
        # Invalid Index
        # {name: "NameIndex", fields: ["firstName", "lastName"]},
        {name: "PrimaryIndex", fields: ["id"], unique: true}
      ]) {
        createdAt: DateTime! @createdAt
        firstName: String!
        id: Int! @id
        lastName: String!
        updatedAt: DateTime! @updatedAt
      }`)

    const createdAtField = new GQLScalarField('createdAt', 'DateTime', true)
    createdAtField.isCreatedAt = true
    const updatedAtField = new GQLScalarField('updatedAt', 'DateTime', true)
    updatedAtField.isUpdatedAt = true
    const idField = new GQLScalarField('id', 'Int', true)
    idField.isId = true
    const firstNameField = new GQLScalarField('firstName', 'String', true)
    const lastNameField = new GQLScalarField('lastName', 'String', true)

    const type: IGQLType = {
      name: 'User',
      isEmbedded: false,
      isLinkTable: false,
      isEnum: false,
      fields: [
        idField,
        createdAtField,
        updatedAtField,
        firstNameField,
        lastNameField,
      ],
      indices: [
        {
          name: 'NameIndex',
          fields: [firstNameField, lastNameField],
          unique: false,
          comments: [{ isError: true, text: 'Invalid Index' }]
        },
        { name: 'PrimaryIndex', fields: [idField], unique: true, comments: [] },
      ],
      directives: [],
      comments: [],
      databaseName: null,
    }

    const res = renderer.render(
      {
        types: [type],
      },
      true,
    )

    expect(res).toEqual(modelWithDirectives)
  })

  test('Render built-in sequence directive correctly', () => {
    const renderer = Renderer.create(DatabaseType.postgres, true)

    const modelWithDirectives = dedent(`
      type User {
        createdAt: DateTime! @createdAt
        firstName: String!
        id: Int! @id(strategy: SEQUENCE) @sequence(name: "test_seq", initialValue: 8, allocationSize: 100)
        lastName: String!
        updatedAt: DateTime! @updatedAt
      }`)

    const createdAtField = new GQLScalarField('createdAt', 'DateTime', true)
    createdAtField.isCreatedAt = true
    const updatedAtField = new GQLScalarField('updatedAt', 'DateTime', true)
    updatedAtField.isUpdatedAt = true
    const idField = new GQLScalarField('id', 'Int', true)
    idField.isId = true
    idField.idStrategy = IdStrategy.Sequence
    idField.associatedSequence = {
      name: 'test_seq',
      initialValue: 8,
      allocationSize: 100,
    }
    const firstNameField = new GQLScalarField('firstName', 'String', true)
    const lastNameField = new GQLScalarField('lastName', 'String', true)

    const type: IGQLType = {
      name: 'User',
      isEmbedded: false,
      isLinkTable: false,
      isEnum: false,
      fields: [
        idField,
        createdAtField,
        updatedAtField,
        firstNameField,
        lastNameField,
      ],
      indices: [],
      directives: [],
      comments: [],
      databaseName: null,
    }

    const res = renderer.render(
      {
        types: [type],
      },
      true,
    )

    expect(res).toEqual(modelWithDirectives)
  })

  test('Render built-in linktable directive correctly', () => {
    const renderer = Renderer.create(DatabaseType.postgres, true)

    const modelWithDirectives = dedent(`
      type User {
        id: Int! @id
        lastName: String!
      }
      
      type UserToUser @linkTable {
        A: User!
        B: User!
      }`)

    const idField = new GQLScalarField('id', 'Int', true)
    idField.isId = true
    const lastNameField = new GQLScalarField('lastName', 'String', true)

    const type: IGQLType = {
      name: 'User',
      isEmbedded: false,
      isLinkTable: false,
      isEnum: false,
      fields: [idField, lastNameField],
      indices: [],
      directives: [],
      comments: [],
      databaseName: null,
    }

    const linkType: IGQLType = {
      name: 'UserToUser',
      isEmbedded: false,
      isLinkTable: true,
      isEnum: false,
      fields: [
        new GQLOneRelationField('A', type, true),
        new GQLOneRelationField('B', type, true),
      ],
      indices: [],
      directives: [],
      comments: [],
      databaseName: null,
    }

    const res = renderer.render(
      {
        types: [type, linkType],
      },
      true,
    )

    expect(res).toEqual(modelWithDirectives)
  })
})
