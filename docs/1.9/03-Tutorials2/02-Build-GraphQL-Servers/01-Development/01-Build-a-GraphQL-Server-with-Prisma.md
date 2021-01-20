---
alias: ohdaiyoo6c
description: Learn how to build a GraphQL server for an existing Prisma service.
---

# Build a GraphQL server with Prisma

This tutorial teaches you how to take an existing Prisma service and build a GraphQL server on top of it. The resolvers of the GraphQL servers will be connected to Prisma's GraphQL API via _Prisma bindings_.

The tutorial assumes that you already have a running Prisma service, so please make sure to have the _endpoint_ of it available. If you're unsure about how you can get started with your own Prisma service, check one of these tutorials:

- [Setup Prisma on a Demo server](!alias-ouzia3ahqu)
- [Setup Prisma with a new MySQL Database](!alias-gui4peul2u)
- [Setup Prisma with a new Postgres Database](!alias-eiyov7erah)
- [Setup Prisma by connecting your empty MySQL Database](!alias-dusee0nore)
- [Setup Prisma by connecting your empty Postgres Database](!alias-aiy1jewith)

<InfoBox>

To ensure you're not accidentally skipping an instruction in the tutorial, all required actions are highlighted with a little _counter_ on the left.

💡 **Pro tip**: If you're only keen on getting started but don't care so much about the explanations of what's going on, you can simply jump from instruction to instruction.

</InfoBox>

## Why not use the Prisma API directly from your client applications?

One commonly asked question is why not to use Prisma's GraphQL API as your entire backend - it has a GraphQL API, so why bother writing another GraphQL server on top of it?

Prisma turns your database into a GraphQL API, exposing powerful CRUD operations to read and modify the data. This means letting your clients talk to Prisma directly is equivalent to directly exposing your entire database to your clients.

There are several reasons why this is not a suitable setup for production use cases:

- Your clients should be able to consume a domain-specific API rather than working with generic CRUD operations
- You want to provide authentication functionality for your users so that they can register with a password or some 3rd-party authentication provider
- You want your API to integrate with microservices or other legacy systems
- You want to include 3-rd party services (such as Stripe, GitHub, Yelp, ...) or other public APIs into your server
- You don't want to expose your entire database schema to everyone (which would be the case due to GraphQL's [introspection](http://graphql.org/learn/introspection/) feature)

## Step 1: Update the data model

You already have your existing Prisma service, but for this tutorial we need to make sure that it has the right data model for the upcoming steps.

Note that we're assuming that your data model lives in a single file called `datamodel.graphql`. If that's not the case, please adjust your setup.

<Instruction>

Open `datamodel.graphql` and update its contents to look as follows:

```graphql
type User {
  id: ID! @unique
  name: String!
  posts: [Post!]!
}

type Post {
  id: ID! @unique
  title: String!
  content: String!
  published: Boolean! @default(value: "false")
  author: User!
}
```

</Instruction>

<Instruction>

After you saved the file, open your terminal and navigate into the root directory of your Prisma service (the one where `prisma.yml` is located) and run the following command to update its GraphQL API:

```sh
prisma deploy
```

</Instruction>

The GraphQL API of your Prisma service now exposes CRUD operations for the `User` as well as the `Post` type that are defined in your data model and also lets you modify _relations_ between them.

## Step 2: Setup the GraphQL server with `graphql-yoga`

Next, you'll create the file structure for your GraphQL server and add the required NPM dependencies to the project. Note that the directory that currently holds the files for your Prisma service (`prisma.yml` and `datamodel.graphql`) will later be located _inside_ the GraphQL server directory.

<Instruction>

Navigate into a new directory and and the following commands in your terminal:

```sh
mkdir -p my-yoga-server/src
touch my-yoga-server/src/index.js
touch my-yoga-server/src/schema.graphql
cd my-yoga-server
yarn init -y
```

</Instruction>

This creates the correct expected structure as well as a `package.json` file inside of it. `index.js` will be the _entry-point_ of your server, `schema.graphql` defines the _application schema_ (the GraphQL schema defining the GraphQL API of your server).

<Instruction>

Next, move the root directory of your Prisma service into `my-yoga-server` and rename it to `prisma`.

</Instruction>

The structure of `my-yoga-server` should now look similar to this:

```
my-yoga-server
│
├── package.json
├── prisma
│   ├── datamodel.graphql
│   └── prisma.yml
└── src
    └── index.js
```

<Instruction>

Next, install the required dependencies in your project:

```sh
yarn add graphql-yoga prisma-binding
```

</Instruction>

Here's an overview of the two dependencies you added:

- `graphql-yoga`: Provides the functionality for your GraphQL server (based on Express.js)
- `prisma-binding`: Easily lets you connect your resolvers to Prisma's GraphQL API

## Step 3: Define the application schema

The API of _every_ GraphQL server is defined by a corresponding [GraphQL schema](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e) which specifies the API operations exposed by the server. It's now time to define the operations of your server's API.

As you might have guessed from the data model, you're building the API for a simple blogging application. As mentioned in the beginning, your clients should not be able to do whatever they like with the `Post` and `User` types, but instead will consume _domain-specific_ operations which are tailored to their needs and the application domain.

<Instruction>

Open `schema.graphql` and add the following schema definition to it:

```graphql
# import Post from './generated/prisma.graphql'
# import User from './generated/prisma.graphql'

type Query {
  posts(searchString: String): [Post!]!
  user(id: ID!): User
}

type Mutation {
  createDraft(authorId: ID!, title: String!, content: String!): Post
  publish(id: ID!): Post
  deletePost(id: ID!): Post
  signup(name: String!): User!
}
```

</Instruction>

This GraphQL schema defines six operations (two queries and four mutations):

- `posts(searchString: String)`: Retrieve the list of all `Post`s and potentially filter them by providing a `searchString`
- `user(id: ID!)`: Retrieve a single `User` by its `id`
- `createDraft(authorId: ID!, title: String!, content: String!)`: Create a new _draft_ (i.e. a `Post` where `published` is set to `false`) written by a specific `User` identified by `authorId`
- `publish(id: ID!)`: Publish a _draft_ (i.e. set its `published` field to `true`)
- `deletePost(id: ID!)`: Delete a `Post`
- `signup(name: String!)`: Create a new `User` by providing a `name`

There's one odd thing about the GraphQL schema right now though: The `Post` and `User` types are _imported_ from the `src/generated/prisma.graphql` which doesn't even exist yet - don't worry you'll download it next. Another thing to notice is that the import syntax uses GraphQL _comments_. These comments must not be deleted! They are used by [`graphql-import`](https://oss.prisma.io/content/GraphQL-Import/Overview.html), a tool that lets you import SDL types _across files_ which is not possible with standard SDL ([yet](https://github.com/graphql/graphql-wg/blob/master/notes/2018-02-01.md#present-graphql-import)!).

## Step 4: Download the Prisma database schema

The next step is to download the GraphQL schema of Prisma's GraphQL API (also referred to as _Prisma database schema_) into your project so you can properly import the SDL types from there.

Technically speaking, this is not absolutely necessary since you could also just _redefine_ identical `Post` and `User` types in `schema.graphql`. However, this would mean that you now have _two_ separate locations where these type definitions live. Whenever you now wanted to update the types, you'd have to do that twice. It is therefore considered _best practice_ to import the types from Prisma's GraphQL schema.

Downloading the Prisma database schema is done using the [GraphQL CLI](https://oss.prisma.io/content/GraphQL-CLI/01-Overview.html) and [GraphQL Config](https://oss.prisma.io/content/GraphQL-Config/Overview.html).

<Instruction>

Install the GraphQL CLI using the following command:

```sh
yarn global add graphql-cli
```

</Instruction>

<Instruction>

Next, create your `.graphqlconfig` in the root directory of the server (i.e. in the `my-yoga-server` directory):

```sh
touch .graphqlconfig.yml
```

</Instruction>

<Instruction>

Put the following contents into it, defining the two GraphQL APIs you're working with in this project (Prisma's GraphQL API as well as the customized API of your `graphql-yoga` server):

```yml
projects:
  app:
    schemPath: src/schema.graphql
    extensions:
      endpoints:
        default: http://localhost:4000
  prisma:
    schemaPath: src/generated/prisma.graphql
    extensions:
      prisma: prisma/prisma.yml
```

</Instruction>

The information you provide in this file is used by the GraphQL CLI as well as the GraphQL Playground. In the Playground specifically it allows you to work with both APIs side-by-side.

<Instruction>

To download the Prisma database schema to `src/generated/prisma.graphql`, run the  following command in your terminal:

```sh
graphql get-schema --project prisma
```

</Instruction>

The Prisma database schema which defines the full CRUD API for your database is now available in the location you specified in the `projects.prisma.schemaPath` property in your `.graphqlconfig.yml` (which is `src/generated/prisma.graphql`) and the import statements will work properly.

<InfoBox>

💡 **Pro tip**: If you want you want the Prisma database schema to update automatically every time you deploy changes to your Prisma services (e.g. an update to the data model), you can add the following post-deployment [hook](!alias-ufeshusai8#hooks-optional) to your `prisma.yml` file:

```yml
hooks:
  post-deploy:
    - graphql get-schema -p prisma
```

</InfoBox>


## Step 5: Instantiate the Prisma binding

The last step before implementing the resolvers for your application schema is to ensure these resolvers can access Prisma's GraphQL API via a Prisma binding.

You'll therefore instantiate a Prisma binding and attach to the `context` object which is passed through the resolver chain.

<Instruction>

Open `index.js` and add the following code to it - note that you need to replace the `__YOUR_PRISMA_ENDPOINT__` placeholder with the endpoint of your Prisma API (which you can find in `prisma.yml`):

```js
const { GraphQLServer } = require('graphql-yoga')
const { Prisma } = require('prisma-binding')

const resolvers = {
  Query: {
    posts: (_, args, context, info) => {
      // ...
    },
    user: (_, args, context, info) => {
      // ...
    }
  },
  Mutation: {
    createDraft: (_, args, context, info) => {
      // ...
    },
    publish: (_, args, context, info) => {
      // ...
    },
    deletePost: (_, args, context, info) => {
      // ...
    },
    signup: (_, args, context, info) => {
      // ...
    }
  }
}

const server = new GraphQLServer({
  typeDefs: 'src/schema.graphql',
  resolvers,
  context: req => ({
    ...req,
    prisma: new Prisma({
      typeDefs: 'src/generated/prisma.graphql',
      endpoint: '__YOUR_PRISMA_ENDPOINT__',
    }),
  }),
})
server.start(() => console.log(`GraphQL server is running on http://localhost:4000`))
```

</Instruction>

You will add the actual implementation for the `resolvers` in a bit. Again, be sure to replace the `__YOUR_PRISMA_ENDPOINT__` placeholder with the endpoint of your Prisma service. This endpoint is specified as the `endpoint` property in your `prisma.yml`.

## Step 6: Implement the resolvers for your GraphQL server using Prisma bindings

By attaching the `Prisma` binding instance to the `context`, all your resolver functions get access to it and can invoke its binding functions. Instead of the resolvers needing to access a database directly, they're effectively _delegating_ the execution of the incoming queries to Prisma's GraphQL API.

Bindings functions provide a convenient API to send queries and mutations to a GraphQL API using JavaScript. The neat thing is that you can pass along the [`info`](https://blog.graph.cool/graphql-server-basics-demystifying-the-info-argument-in-graphql-resolvers-6f26249f613a) object which contains the information about which fields a client requested in its query - that way, Prisma can resolve the query very efficiently.

<Instruction>

Inside `index.js`, add implementations for the resolver functions like so:

```js
const resolvers = {
  Query: {
    posts: (_, args, context, info) => {
      return context.prisma.query.posts(
        {
          where: {
            OR: [
              { title_contains: args.searchString },
              { content_contains: args.searchString },
            ],
          },
        },
        info,
      )
    },
    user: (_, args, context, info) => {
      return context.prisma.query.user(
        {
          where: {
            id: args.id,
          },
        },
        info,
      )
    },
  },
  Mutation: {
    createDraft: (_, args, context, info) => {
      return context.prisma.mutation.createPost(
        {
          data: {
            title: args.title,
            content: args.content,
            author: {
              connect: {
                id: args.authorId,
              },
            },
          },
        },
        info,
      )
    },
    publish: (_, args, context, info) => {
      return context.prisma.mutation.updatePost(
        {
          where: {
            id: args.id,
          },
          data: {
            published: true,
          },
        },
        info,
      )
    },
    deletePost: (_, args, context, info) => {
      return context.prisma.mutation.deletePost(
        {
          where: {
            id: args.id,
          },
        },
        info,
      )
    },
    signup: (_, args, context, info) => {
      return context.prisma.mutation.createUser(
        {
          data: {
            name: args.name,
          },
        },
        info,
      )
    },
  },
}
```

</Instruction>

The implementation of each resolver simply is an invocation of a binding function to delegate the operation down to the Prisma API, saving you from having to perform any manual database access.

## Step 7: Start the server and use the API in a Playground

The implementation of your GraphQL server is now done and you can work with it inside a GraphQL Playground.

<Instruction>

In your terminal, start the server using the following command:

```sh
node src/index.js
```

</Instruction>

Now, you can open a GraphQL Playground either by navigating to `http://localhost:4000` in your browser.

<InfoBox>

💡 **Pro tip:** Instead of directly accessing the URL in your browser, you can also use the `graphql playground` command which will read the information from your `.graphqlconfig.yml` file and lets you work with both GraphQL APIs side-by-side.

</InfoBox>

To test the API, you can send the following queries and mutations:

**Create a new user**:

```graphql
mutation {
  signup(name: "Alice") {
    id
  }
}
```

**Create a new draft for a User**:

Note that you need to replace the `__USER_ID__` placeholder with the `id` of an actual `User` from your database.

```graphql
mutation {
  createDraft(
    title: "Join us at GraphQL Europe 🇪🇺 ",
    content: "Get a 10%-discount with this promo code on graphql-europe.org: gql-boilerplates",
    authorId: "__USER_ID__"
  ) {
    id
    published
  }
}
```

**Publish a draft**:

Note that you need to replace the `__POST_ID__` placeholder with the `id` of an actual `Post` from your database.

```graphql
mutation {
  publish(
    id: "__POST_ID__",
  ) {
    id
    published
  }
}
```

**Filter posts for "GraphQL Europe"**

```graphql
query {
  posts(searchString: "GraphQL Europe") {
    id
    title
    content
    published
    author {
      id
      name
    }
  }
}
```

**Delete a post**:

Note that you need to replace the `__POST_ID__` placeholder with the `id` of an actual `Post` from your database.

```graphql
mutation {
  deletePost(
    id: "__POST_ID__",
  ) {
    id
  }
}
```
