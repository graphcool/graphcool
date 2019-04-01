# prisma-db-introspection

This module is capable of generating a prisma datamodel for a relational or document databases. Please refer to the `prisma-datamodel` doc for more information on the `ISDL` datamodel structure.

## Convenient Shorthand usage

Iintrospection and rendering in one step):

```typescript
const renderedSdl = await connector
  .introspect(schema)
  .renderToNormalizdDatamodelString()
```

Creating a connector:

```typescript
const connector = Connectors.create(DatabaseType.mysql, client)
const connector = Connectors.create(DatabaseType.postgres, client)
const connector = Connectors.create(DatabaseType.mongo, client)
```

The database client has to be connected and disconnected by the caller. Please refer to the connector implementation to see the required client library.

## Detailed Usage

Introspect the database:

```typescript
const introspection = await connector.introspect(schema)
```

The introspection result caches all database related information and is database specific.

Create an `ISDL` structure from the introspection:

```typescript
const sdl: ISDL = await introspection.getNormalizedDatamodel()
```

or with an existing reference model:

```typescript
const sdl: ISDL = await introspection.getNormalizedDatamodel(referenceModel)
```

it is also possible to get an unnormalized datamodel, basically a raw introspection result from the database. The unnormalized model is most likely not a valid datamodel.

```typescript
const sdl: ISDL = await introspection.getDatamodel() // Don't use unless you know what you're doing
```

Rendering can be done using `prisma-datamodel`.

With prototype features enabled (V1.1):

```typescript
const renderer = Renderers.create(introspection.databaseType, true)
const renderedSdl = renderer.render(sdl)
```

Without prototype features, simply use the shorthand:

```typescript
const renderer = Renderers.create(introspection.databaseType)
const renderedSdl = renderer.render(sdl)
```

Which is equivalent to, given the database type for rendering and introspection are the same:

```typescript
const renderedSdl = introspection.renderToDatamodelString()
```

Or with an existing reference model:

```typescript
const renderedSdl = introspection.renderToNormalizedDatamodelString(
  referenceModel,
)
```

### Document Database Introspection Internals

Document database introspection works by randomly sampling objects from each collection, inferring a schema for each object and merging these object schemas together. This is done in the class `ModelSampler`.

For each field which might be a reference to another collection, we attempt to look up objects in other collections by their primary key. If we find enough referred objects, we mark the field as relation. This is done in `RelationResolver`, and can be a somewhat expensive operation.

This approach should work for all forms of document DBs. The `MongoConnector` class can be used as a reference implementation.

### Relational Database Introspection Internals

Relational introspection works by querying the database schema using SQL, and then bringing everything together.

Relations are resolved via their corresponding FK constraints, IDs are resolved via their corresponding PK constraints.

Here, `MysqlConnector` , `MySqlIntrospectionResult` , `PostgresConnector` and `PostgresIntrospectionResult` can serve as a reference.

There is a common base class, `RelationalConnector` which attempts to unify certain queries using the `information_schema` table, which should be standardized. This approach had limited success in practice.

### Normalization Pipeline

The exact normalization pipeline can be found in the `DefaultNormalizer` factory class. In the most complex case, introspecting a relational database with an existing base model, the following steps are applied:

1. Copy Enum definitions from the existing base model.
2. Remove all relation names which are not needed, unless they are explicitly given by the base model.
3. Normalize all type and field names. If the type or field is present in the base model, copy the name and directives, as they might be known to prisma, but not to the database.
4. Re-order all models according to the base model. For new types and enums, order alphabetically.
5. Hide all reserved fields, like `updatedAt`, `createdAt`, `id`, unless they are present in the base model.
6. Adjust the cardinality of relations which use a join table according to the base model, since we cannot guess them from the database.
7. Remove all back relations for inine relations, except they are given in the datamodel. This is especially important for self-relations, which would otherwise generate duplicated fields.

### Testing

There is a `docker-compose.yml` in the root directory, which spawns all databases needed for tests.

The environment for use with this setup is the following:

```
export TEST_PG_DB=prisma
export TEST_PG_HOST=localhost
export TEST_PG_USER=prisma
export TEST_PG_PASSWORD=prisma
export TEST_PG_PORT=5433

export TEST_MONGO_URI=mongodb://prisma:localhost:27018@mongo
export TEST_MONGO_SCHEMA=SchemaIntrospectionTest

export TEST_MYSQL_HOST=localhost
export TEST_MYSQL_PORT=3307
export TEST_MYSQL_USER=root
export TEST_MYSQL_PASSWORD=prisma⏎
```
