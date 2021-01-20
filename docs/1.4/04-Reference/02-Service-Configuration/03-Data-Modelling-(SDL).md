---
alias: eiroozae8u
description: An overview of how to design data models with Prisma.
---

# Data Modelling

## Overview

Prisma uses the GraphQL [Schema Definition Language](https://blog.graph.cool/graphql-sdl-schema-definition-language-6755bcb9ce51) (SDL) for data modeling. Your data model is written in one or more `.graphql`-files and is the foundation for the actual database schema that Prisma generates under the hood. If you're using just a single file for your type definitions, this file is typically called `datamodel.graphql`.

> To learn more about the SDL, you can check out the [official GraphQL documentation](http://graphql.org/learn/schema/#type-language).

The `.graphql`-files containing the data model need to be specified in `prisma.yml` under the `datamodel` property. For example:

```yml
datamodel:
  - types.graphql
  - enums.graphql
```

If there is only a single file that defines the data model, it can be specified as follows:

```yml
datamodel: datamodel.graphql
```

The data model is the foundation for the GraphQL API of your Prisma service. Based on the data model, Prisma will generate a powerful [GraphQL schema](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e) (called **Prisma database schema**) which defines CRUD operations for the types in the data model.

<InfoBox>

A GraphQL schema defines the operations of a GraphQL API. It effectively is a collection of _types_ written in SDL (the SDL also supports primitives like interfaces, enums, union types and more, you can learn everything about GraphQL's type system [here](http://graphql.org/learn/schema/#type-system)). A GraphQL schema has three special _root types_: `Query`, `Mutation` and `Subscription`. These types define the _entry points_ for the API and define what operations the API will accept. To learn more about GraphQL schema, check out this [article](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e).

</InfoBox>

### Example

A simple `datamodel.graphql` file:

```graphql
type Tweet {
  id: ID! @unique
  createdAt: DateTime!
  text: String!
  owner: User!
  location: Location!
}

type User {
  id: ID! @unique
  createdAt: DateTime!
  updatedAt: DateTime!
  handle: String! @unique
  name: String
  tweets: [Tweet!]!
}

type Location {
  latitude: Float!
  longitude: Float!
}
```

This example illustrates a few important concepts when working with your data model:

* The three types `Tweet`, `User` and `Location` are mapped to tables in the database.
* There is a bidirectional relation between `User` and `Tweet`
* There is a unidirectional relation from `Tweet` to `Location`
* Except for the `name` field on `User`, all fields are required in the data model (as indicated by the `!` following the type).
* The `id`, `createdAt` and `updatedAt` fields are managed by Prisma and read-only in the exposed GraphQL API (meaning they can not be altered via mutations).

Creating and updating your data model is as simple as writing a text file. Once you're happy with your data model, you can apply the changes to your Prisma service by running `prisma deploy`:

```sh
$ prisma deploy

Changes:

  Tweet (Type)
  + Created type `Tweet`
  + Created field `id` of type `GraphQLID!`
  + Created field `createdAt` of type `DateTime!`
  + Created field `text` of type `String!`
  + Created field `owner` of type `Relation!`
  + Created field `location` of type `Relation!`
  + Created field `updatedAt` of type `DateTime!`

  User (Type)
  + Created type `User`
  + Created field `id` of type `GraphQLID!`
  + Created field `createdAt` of type `DateTime!`
  + Created field `updatedAt` of type `DateTime!`
  + Created field `handle` of type `String!`
  + Created field `name` of type `String`
  + Created field `tweets` of type `[Relation!]!`

  Location (Type)
  + Created type `Location`
  + Created field `latitude` of type `Float!`
  + Created field `longitude` of type `Float!`
  + Created field `id` of type `GraphQLID!`
  + Created field `updatedAt` of type `DateTime!`
  + Created field `createdAt` of type `DateTime!`

  TweetToUser (Relation)
  + Created relation between Tweet and User

  LocationToTweet (Relation)
  + Created relation between Location and Tweet

Applying changes... (22/22)
Applying changes... 0.4s
```

### Building blocks of the data model

There are several available building blocks to shape your data model.

* [Types](#object-types) consist of multiple [fields](#fields) and are used to group similar entities together. Each type in your data model is mapped to the database and CRUD operations are added to the GraphQL schema.
* [Relations](#relations) describe _relationships_ between types.
* [Interfaces](http://graphql.org/learn/schema/#interfaces) are abstract types that include a certain set of fields which a type must include to _implement_ the interface. Currently, interfaces cannot be user-defined, but [there's a pending feature request](https://github.com/graphcool/framework/issues/83) for advanced interface support.
* Special [directives](#graphql-directives) covering different use cases such as type constraints or cascading delete behaviour.

The rest of this page describes these building blocks in more detail.

## Prisma database schema vs Data model

When starting out with GraphQL and Prisma, the amount of `.graphql`-files you're working with can be confusing. Yet, it's crucial to understand what the role of each of them is.

In general, a `.graphql`-file can contain either of the following:

- GraphQL operations (i.e. _queries_, _mutations_ or _subscriptions_)
- GraphQL type definitions in SDL

In the context of distinguishing the Prisma database schema from the data model, only the latter is relevant!

Note that not every `.graphql`-file that falls into the latter category is per se a _valid_ GraphQL schema. As mentioned in the info box above, a GraphQL schema is characterised by the fact that it has three root types: `Query`, `Mutation` and `Subscription` in addition to any other types that are required for the API.

Now, by that definition the **data model is not actually a GraphQL schema**, despite being a `.graphql`-file written in SDL. It lacks the root types and thus doesn't actually define API operations! Prisma simply uses the data model as a handy tool for you to express what the data model looks like.

As mentioned above, Prisma will then generate an actual GraphQL schema that contains the `Query`, `Mutation` and `Subscription` root types. This schema is typically stored inside your project as `prisma.graphql` and called the **Prisma database schema**. Note that you should never make any manual changes to this file!

As an example, consider the following very simple data model:

**`datamodel.graphql`**

```graphql
type User {
  id: ID! @uniue
  name: String!
}
```

If you're deploying this data model to your Prisma service, Prisma will generate the following Prisma database schema that defines the GraphQL API of your service:

**`prisma.graphql`**

```graphql
type Query {
  users(where: UserWhereInput, orderBy: UserOrderByInput, skip: Int, after: String, before: String, first: Int, last: Int): [User]!
  user(where: UserWhereUniqueInput!): User
}

type Mutation {
  createUser(data: UserCreateInput!): User!
  updateUser(data: UserUpdateInput!, where: UserWhereUniqueInput!): User
  deleteUser(where: UserWhereUniqueInput!): User
}

type Subscription {
  user(where: UserSubscriptionWhereInput): UserSubscriptionPayload
}
```

Note that this is a simplified version of the generated schema, you can find the full schema [here](https://gist.github.com/gc-codesnippets/f302c104f2806f9e13f41d909e07d82d).

<InfoBox>

If you've already looked into building your own GraphQL server based on Prisma, you might have come across another `.graphql`-file which is referred to as your **application schema**. This is another proper GraphQL schema (meaning it contains the `Query`, `Mutation` and `Subscription` root types) that defines the API exposed to your client applications. It uses the underlying Prisma GraphQL API as a "query engine" to actually run the queries, mutations and subscriptions against the database.

A GraphQL server based on Prisma usually has two GraphQL APIs, think of them as two layers for your service:

- **Application layer**: Defined by the application schema (here is where you implement business logic, authentication, integrate with 3rd-party services, etc)
- **Database layer**: Defined by the Prisma database service

</InfoBox>

## Object types

An _object type_ (or short _type_) defines the structure for one concrete part of your data model. It is used to represents _entities_ from your _application domain_.

 If you are familiar with SQL databases you can think of an object type as the schema for a _table_ in your relational database. A type has a _name_ and one or multiple _[fields](#fields)_.

An instantiation of a type is called a _node_. This term refers to a node inside your _data graph_.
Every type you define in your data model will be available as an analogous type in the generated _Prisma database schema_.

### Defining an object type

A object type is defined in the data model with the keyword `type`:

```graphql
type Article {
  id: ID! @unique
  text: String!
  isPublished: Boolean @default(value: false)
}
```

The type defined above has the following properties:

* Name: `Article`
* Fields: `id`, `text` and `isPublished` (with the default value `false`)

### Generated API operations for types

The types in your data model affect the available operations in the [Prisma GraphQL API](!alias-abogasd0go). For every type,

* [queries](!alias-ahwee4zaey) allow you to fetch one or many nodes of that type
* [mutations](!alias-ol0yuoz6go) allow you to create, update or delete nodes of that type
* [subscriptions](!alias-aey0vohche) allow you to get notified of changes to nodes of that type (i.e. new nodes are _created_ or existing nodes are _updated_ or _deleted_)

## Fields

_Fields_ are the building blocks of a [type](#object-types), giving a node its _shape_. Every field is referenced by its name and is either [scalar](#scalar-types) or a [relation](#relations) field.

### Scalar types

#### String

A `String` holds text. This is the type you would use for a username, the content of a blog post or anything else that is best represented as text.

Note: String values are currently limited to 256KB in size on the shared demo cluster. This limit can be increased on other clusters using [the cluster configuration](https://github.com/graphcool/framework/issues/748).

In queries or mutations, String fields have to be specified using enclosing double quotes: `string: "some-string"`.

#### Integer

An `Int` is a number that cannot have decimals. Use this to store values such as the weight of an ingredient required for a recipe or the minimum age for an event.

Note: `Int` values range from -2147483648 to 2147483647.

In queries or mutations, `Int` fields have to be specified without any enclosing characters: `int: 42`.

#### Float

A `Float` is a number that can have decimals. Use this to store values such as the price of an item in a store or the result of complex calculations.

In queries or mutations, `Float` fields have to be specified without any enclosing characters and an optional decimal point: `float: 42`, `float: 4.2`.

#### Boolean

A `Boolean` can have the value `true` or `false`. This is useful to keep track of settings such as whether the user wants to receive an email newsletter or if a recipe is appropriate for vegetarians.

In queries or mutations, `Boolean` fields have to be specified without any enclosing characters: `boolean: true`, `boolean: false`.

#### DateTime

The `DateTime` type can be used to store date or time values. A good example might be a person's date of birth.

In queries or mutations, `DateTime` fields have to be specified in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) with enclosing double quotes:

* `datetime: "2015"`
* `datetime: "2015-11"`
* `datetime: "2015-11-22"`
* `datetime: "2015-11-22T13:57:31.123Z"`.

#### Enum

Enums are defined on a service scope.

Like a Boolean an Enum can have one of a predefined set of values. The difference is that you can define the possible values. For example you could specify how an article should be formatted by creating an Enum with the possible values `COMPACT`, `WIDE` and `COVER`.

Note: Enum values can at most be 191 characters long.

In queries or mutations, Enum fields have to be specified without any enclosing characters. You can only use values that you defined for the enum: `enum: COMPACT`, `enum: WIDE`.

#### Json

Sometimes you need to store arbitrary Json values for loosely structured data. The `Json` type makes sure that it is actually valid Json and returns the value as a parsed Json object/array instead of a string.

Note: Json values are currently limited to 256KB in size on the shared demo cluster. This limit can be increased on other clusters using [the cluster configuration](https://github.com/graphcool/framework/issues/748).

In queries or mutations, Json fields have to be specified with enclosing double quotes. Special characters have to be escaped: `json: "{\"int\": 1, \"string\": \"value\"}"`.

#### ID

An ID value is a generated unique 25-character string based on [cuid](https://github.com/graphcool/cuid-java). Fields with ID values are system fields and just used internally, therefore it is not possible to create new fields with the ID type.

### Type modifiers

#### List

Scalar fields can be marked with the list field type. A field of a relation that has the many multiplicity will also be marked as a list.

In queries or mutations, list fields have to be enclosed by square brackets, while the separate entries of the list adhere to the same formatting rules as lined out above: `listString: ["a string", "another string"]`, `listInt: [12, 24]`.

#### Required

Fields can be marked as required (sometimes also referred to as "non-null"). When creating a new node, you need to supply a value for fields which are required and don't have a [default value](#default-value).

Required fields are marked using a `!` after the field type: `name: String!`.

### Field constraints

Fields can be configured with certain field constraints to add further semantics to your data model.

#### Unique

Setting the _unique_ constraint makes sure that two nodes of the type in question cannot have the same value for a certain field. The only exception is the `null` value, meaning that multiple nodes can have the value `null` without violating the constraint.

> A typical example would be an `email` field on the `User` type where the assumption is that every `User` should have a globally unique email address.

Please note that only the first 191 characters in a String field are considered for uniqueness and the unique check is **case insensitive**. Storing two different strings is not possible if the first 191 characters are the same or if they only differ in casing.

To mark a field as unique, simply append the `@unique` directive to it:

```graphql
type User {
  email: String! @unique
  age: Int!
}
```

For every field that's annotated with `@unique`, you're able to query the corresponding node by providing a value for that field.

For example, for the above data model, you can now retrieve a particular `User` node by its `email` address:

```graphql
query {
  user(where: {
    email: "alice@graph.cool"
  }) {
    age
  }
}
```

#### More constraints

More database constraints will be added going forward according to this [feature request](https://github.com/graphcool/graphcool/issues/728).

### Default value

You can set a default value for scalar fields. The value will be taken for new nodes when no value was supplied during creation.

To specify a default value for a field, you can use the `@default` directive:

```graphql
type Story {
  isPublished: Boolean @default(value: "false")
  someNumber: Int! @default(value: "42")
  title: String! @default(value: "My New Post")
  publishDate: DateTime! @default(value: "2018-01-26")
}
```

Notice that you need to always provide the value in double-quotes, even for non-string types such as `Boolean` or `Int`.

### System fields

The three fields `id`, `createdAt` and `updatedAt` have special meaning. They are optional in your data model, but will always be maintained in the underlying database. This way you can always add the field to your data model later, and the data will be available for existing nodes.

> The values of these fields are currently read-only in the GraphQL API (except when [importing data](!alias-ol2eoh8xie)) but will be made configurable in the future. See [this proposal](https://github.com/graphcool/framework/issues/1278) for more information.

<InfoBox type=warning>

Notice that you cannot have custom fields that are called `id`, `createdAt` and `updatedAt` since these field names are reserved for the system fields. Here are the only supported declarations for these three fields:

* `id: ID! @unique`
* `createdAt: DateTime!`
* `updatedAt: DateTime!`

</InfoBox>

#### System field: `id`

A node will automatically get assigned a globally unique identifier when it's created, this identifier is stored in the `id` field.

Whenever you add the `id` field to a type definition to expose it in the GraphQL API, you must annotate it with the `@unique` directive.

The `id` has the following properties:

* Consists of 25 alphanumeric characters (letters are always lowercase)
* Always starts with a (lowercase) letter `c`
* Follows [cuid](https://github.com/ericelliott/cuid) (_collision resistant unique identifiers_) scheme

Notice that all your object types will implement the `Node` interface in the database schema. This is what the `Node` interface looks like:

```graphql
interface Node {
  id: ID! @unique
}
```

#### System fields: `createdAt` and `updatedAt`

The data model further provides two special fields which you can add to your types:

* `createdAt: DateTime!`: Stores the exact date and time for when a node of this object type was _created_.
* `updatedAt: DateTime!`: Stores the exact date and time for when a node of this object type was _last updated_.

If you want your types to expose these fields, you can simply add them to the type definition, for example:

```graphql
type User {
  id: ID! @unique
  createdAt: DateTime!
  updatedAt: DateTime!
}
```

### Generated API operations for fields

Fields in the data model affect the available [query arguments](!alias-ahwee4zaey#query-arguments).

## Relations

A _relation_ defines the semantics of a connection between two [types](#object-types). Two types in a relation are connected via a [relation field](#scalar-and-relation-fields). When a relation might be ambiguous, the relation field needs to be annotated with the [`@relation`](#relation-fields) directive to disambiguate it.

A relation can also connect a type with itself. It is then referred to as a _self-relation_.

### Required relations

For a _to-one_ relation field, you can configure whether it is _required_ or _optional_. The required flag acts as a contract in GraphQL that this field can never be `null`. A field for the address of a user would therefore be of type `Address` or `Address!`.

Nodes for a type that contains a required _to-one_ relation field can only be created using a [nested mutation](!alias-ol0yuoz6go#nested-mutations) to ensure the according field will not be `null`.

> Note that a _to-many_ relation field is always set to required. For example, a field that contains many user addresses always uses the type `[Address!]!` and can never be of type `[Address!]`. The reason is that in case the field doesn't contain any nodes, `[]` will be returned, which is not `null`.

### The `@relation` directive

When defining relations between types, there is the `@relation` directive which provides meta-information about the relation. It can take two arguments:

* `name`: An identifier for this relation (provided as a string). This argument is only required if relations are ambiguous. Note that the `name` argument is required every time you're using the `@relation` directive.
* `onDelete`: Specifies the _deletion behaviour_ and enables _cascading deletes_. In case a node with related nodes gets deleted, the deletion behaviour determines what should happen to the related nodes. The input values for this argument are defined as an enum with the following possible values:
  * `SET_NULL` (default): Set the related node(s) to `null`.
  * `CASCADE`: Delete the related node(s). Note that is not possible to set _both_ ends of a bidirectional relation to `CASCADE`.

> **Note**:

Here is an example of a data model where the `@relation` directive is used:

```graphql
type User {
  id: ID! @unique
  stories: [Story!]! @relation(name: "StoriesByUser" onDelete: CASCADE)
}

type Story {
  id: ID! @unique
  text: String!
  author: User @relation(name: "StoriesByUser")
}
```

The deletion behaviour in this example is as follows:

- When a `User` node gets deleted, all its related `Story` nodes will be deleted as well.
- When a `Story` node gets deleted, it will simply be removed from the `stories` list on the related `User` node.

#### Omitting the `@relation` directive

In the simplest case, where a relation between two types is unambiguous and the default deletion behaviour (`SET_NULL`) should be applied, the corresponding relation fields do not have to be annotated with the `@relation` directive.

Here we are defining a bidirectional _one-to-many_ relation between the `User` and `Story` types. Since `onDelete` has not been provided, the default deletion behaviour is used: `SET_NULL`:

```graphql
type User {
  id: ID! @unique
  stories: [Story!]!
}

type Story {
  id: ID! @unique
  text: String!
  author: User
}
```

The deletion behaviour in this example is as follows:

- When a `User` node gets deleted, the `author` field on all its related `Story` nodes will be set to `null`. Note that if the `author` field was marked as [required](#required), the operation would result in an error.
- When a `Story` node gets deleted, it will simply be removed from the `stories` list on the related `User` node.

#### Using the `name` argument of the `@relation` directive

In certain cases, your data model may contain ambiguous relations. For example, consider you not only want a relation to express the "author-relationship" between `User` and `Story`, but you also want a relation to express which `Story` nodes have been _liked_ by a `User`.

In that case, you end up with two different relations between `User` and `Story`! In order to disambiguate them, you need to give the relation a name:

```graphql
type User {
  id: ID! @unique
  writtenStories: [Story!]! @relation(name: "WrittenStories")
  likedStories: [Story!]! @relation(name: "LikedStories")
}

type Story {
  id: ID! @unique
  text: String!
  author: User! @relation(name: "WrittenStories")
  likedBy: [User!]! @relation(name: "LikedStories")
}
```

If the `name` wasn't provided in this case, there would be no way to decide whether `writtenStories` should relate to the `author` or the `likedBy` field.

#### Using the `onDelete` argument of the `@relation` directive

As mentioned above, you can specify a dedicated deletion behaviour for the related nodes. That's what the `onDelete` argument of the `@relation` directive is for.

Consider the following example:

```graphql
type User {
  id: ID! @unique
  comments: [Comment!]! @relation(name: "CommentAuthor", onDelete: CASCADE)
  blog: Blog @relation(name: "BlogOwner", onDelete: CASCADE)
}

type Blog {
  id: ID! @unique
  comments: [Comment!]! @relation(name: "Comments", onDelete: CASCADE)
  owner: User! @relation(name: "BlogOwner", onDelete: SET_NULL)
}

type Comment {
  id: ID! @unique
  blog: Blog! @relation(name: "Comments", onDelete: SET_NULL)
  author: User @relation(name: "CommentAuthor", onDelete: SET_NULL)
}
```

Let's investigate the deletion behaviour for the three types:

- When a `User` node gets deleted,
  - all related `Comment` nodes will be deleted.
  - the related `Blog` node will be deleted.
- When a `Blog` node gets deleted,
  - all related `Comment` nodes will be deleted.
  - the related `User` node will have its `blog` field set to `null`.
- When a `Comment` node gets deleted,
  - the related `Blog` node continues to exist and the deleted `Comment` node is removed from its `comments` list.
  - the related `User` node continues to exist and the deleted `Comment` node is removed from its `comments` list.

### Generated API operations for relations

The relations that are included in your schema affect the available operations in the [GraphQL API](!alias-abogasd0go). For every relation,

* [relation queries](!alias-ahwee4zaey#querying-data-across-relations) allow you to query data across types or aggregated for a relation (note that this is also possible using [Relay](https://facebook.github.io/relay/)'s [connection model](!alias-ahwee4zaey#connection-queries))
* [nested mutations](!alias-ol0yuoz6go#nested-mutations) allow you to create, connect, update, upsert and delete nodes across types
* [relation subscriptions](!alias-aey0vohche#relation-subscriptions) allow you to get notified of changes to a relation

## GraphQL directives

Directives are used to provide additional information in your data model. They look like this: `@name(argument: "value")` or simply `@name` when there are no arguments.

### Data model directives

Data model directives describe additional information about types or fields in the GraphQL schema.

#### Unique scalar fields

The `@unique` directive marks a scalar field as [unique](#unique). Unique fields will have a unique _index_ applied in the underlying database.

```graphql
# the `User` type has a unique `email` field
type User {
  email: String @unique
}
```

Find more info about the `@unique` directive [above](#unique).

#### Relation fields

The directive `@relation(name: String, onDelete: ON_DELETE! = NO_ACTION)` can be attached to a relation field.

[See above](#the-relation-directive) for more information.

#### Default value for scalar fields

The directive `@default(value: String!)` sets [a default value](#default-value) for a scalar field. Note that the `value` argument is of type String for all scalar fields (even if the fields themselves are not strings):

```graphql
# the `title`, `published` and `someNumber` fields have default values `New Post`, `false` and `42`
type Post {
  title: String! @default(value: "New Post")
  published: Boolean! @default(value: "false")
  someNumber: Int! @default(value: "42")
}
```

### Temporary directives

Temporary directives are used to perform one-time migration operations. After deploying a service that contain a temporary directive, it **needs to be manually removed from the type definitions file**.

#### Renaming a type or field

The temporary directive `@rename(oldName: String!)` is used to rename a type or field.

```graphql
# renaming the `Post` type to `Story`, and its `text` field to `content`
type Story @rename(oldName: "Post") {
  content: String @rename(oldName: "text")
}
```

<InfoBox type="warning">

If the rename directive is not used, Prisma would remove the old type and field before creating the new one, resulting in loss of data!

</InfoBox>

#### Migrating the value of a scalar field

The temporary directive `@migrationValue(value: String!)` is used to migrate the value of a scalar field. When changing an optional field to a required field, it is necessary to also use this directive.

## Naming conventions

Different objects you encounter in a Prisma service like types or relations follow separate naming conventions to help you distinguish them.

### Types

The type name determines the name of derived queries and mutations as well as the argument names for nested mutations. Type names can only contain **alphanumeric characters** and need to start with an uppercase letter. They can contain **at most 64 characters**.

_It's recommended to choose type names in the singular form._

_Type names are unique on a service level._

##### Examples

* `Post`
* `PostCategory`

### Scalar and relation fields

The name of a scalar field is used in queries and in query arguments of mutations. Field names can only contain **alphanumeric characters** and need to start with a lowercase letter. They can contain **at most 64 characters**.

The name of relation fields follows the same conventions and determines the argument names for relation mutations.

_It's recommended to only choose plural names for list fields_.

_Field names are unique on a type level._

##### Examples

* `name`
* `email`
* `categoryTags`

### Relations

Relation names can only contain **alphanumeric characters** and need to start with an uppercase letter. They can contain **at most 64 characters**.

_Relation names are unique on a service level._

##### Examples

* `UserOnPost`, `UserPosts` or `PostAuthor`, with field names `user` and `posts`
* `Appointments`, `EmployeeOnAppointment` or `AppointmentEmployee`, with field names `employee` and `appointments`

### Enums

Enum values can only contain **alphanumeric characters and underscores** and need to start with an uppercase letter. The name of an enum value can be used in query filters and mutations. They can contain **at most 191 characters**.

_Enum names are unique on a service level._

_Enum value names are unique on an enum level._

##### Examples

* `A`
* `ROLE_TAG`
* `RoleTag`

## More SDL features

In this section, we describe further SDL features that are not yet supported for data modelling with Prisma.

### Interfaces

"Like many type systems, GraphQL supports interfaces. An interface is an abstract type that includes a certain set of fields that a type must include to implement the interface." From the official [GraphQL Documentation](http://graphql.org/learn/schema/#interfaces)

> **Note**: To learn more about when and how interfaces are coming to Prisma, check out this [feature request](https://github.com/graphcool/prisma/issues/83).

### Union types

"Union types are very similar to interfaces, but they don't get to specify any common fields between the types." From the official [GraphQL Documentation](http://graphql.org/learn/schema/#union-types)

> **Note**: To learn more about when and how union types are coming to Prisma, check out this [feature request](https://github.com/graphcool/prisma/issues/165).
