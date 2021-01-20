---
alias: nahgaghei6
description: Learn how to build a GraphQL server from scratch.
---

# Build a GraphQL server from scratch

A GraphQL server can be implemented in various ways. In this tutorial, you’ll learn how to build your own GraphQL server using graphql-yoga from scratch. The final code for this project can be found [here](https://github.com/nikolasburk/blogr).

`graphql-yoga` is a fully-featured GraphQL server library with focus on simple setup, performance and a great developer experience. It’s the easiest way to build GraphQL servers.

![](https://cdn-images-1.medium.com/max/2240/1*lEPK4jHpYVnJiR1zIA62uw.png)

<InfoBox>

💡 This will be a very practical tutorial. If you’d like to dive deeper in any of the mentioned concepts, we encourage you to check out our article series GraphQL Server Basics - The Structure and Implementation of GraphQL Servers:

1. [**The GraphQL Schema**](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e) (1)
1. [**The Network Layer**](https://blog.graph.cool/graphql-server-basics-the-network-layer-51d97d21861) (2)
1. [**Demystifying the info argument in GraphQL resolvers**](https://blog.graph.cool/graphql-server-basics-demystifying-the-info-argument-in-graphql-resolvers-6f26249f613a) (3)

</InfoBox>

## Choosing a GraphQL server library

When starting out with your GraphQL server with Node & [Express](https://expressjs.com/), you’re confronted with the choice between a number of libraries. The most popular ones being:

* [`express-graphql`](https://github.com/graphql/express-graphql) is Facebook’s Express middleware released in 2015, alongside the official GraphQL spec and [GraphQL.js](http://graphql.org/graphql-js/) reference implementation. This was the first library that helped developers to build GraphQL servers.
* [`graphql-yoga`](https://github.com/graphcool/graphql-yoga) builds upon a number of other libraries (such as express, apollo-server, graphql-subscriptions and graphql-playground) and creates a great mix of convenience and flexibility with its simple and extensible API.
* [`apollo-server-express`](https://github.com/apollographql/apollo-server/tree/master/packages/apollo-server-express) is the Express version of `apollo-server` and also built upon Facebook’s GraphQL.js.

## Building a GraphQL server with `graphql-yoga`

In this tutorial, you’ll build the API for a simple blogging application. You’ll start from scratch and add more functionality to the app step-by-step.

### Setting up the project directory

Let’s get started with the tutorial and setup the project!

<Instruction>

In a directory of your choice, run the following commands:

```bash
mkdir blogr
cd blogr
npm init -y
```

</Instruction>

This creates a new directory called `blogr` and adds a `package.json` to it so we can start installing NPM dependencies.

Next, you’ll create the entry-point for the server in a file called `index.js`.

<Instruction>

Inside the `blogr` directory, run the following commands:

```bash
mkdir src
touch src/index.js
```

</Instruction>

<Instruction>

Now, go ahead and install the `graphql-yoga` dependency:

```bash
yarn add graphql-yoga
```

</Instruction>

Awesome, that’s it! You’re all set, let’s write the first lines of code 🙌

### Configuring `graphql-yoga` & Writing your first resolver

The core primitive provided by `graphql-yoga` is a class called [`GraphQLServer`](https://github.com/graphcool/graphql-yoga#graphqlserver). It is configured with everything related to the [GraphQL schema](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e) as well as the web server configuration, such as the *port* it’s running on or its [*CORS*](https://blog.graph.cool/enabling-cors-for-express-graphql-apollo-server-1ef999bfb38d) setup.

For now, you’ll simply instantiate it with a GraphQL schema definition and the corresponding resolver implementation.

<Instruction>

Add the following code to `src/index.js`:

```js
const { GraphQLServer } = require('graphql-yoga')

const typeDefs = `
type Query {
  description: String
}
`

const resolvers = {
  Query: {
    description: () => `This is the API for a simple blogging application`
  }
}

const server = new GraphQLServer({
  typeDefs,
  resolvers
})

server.start(() => console.log(`The server is running on http://localhost:4000`))
```

</Instruction>

Here’s is what’s going on in this snippet:

* `typeDefs` contains our GraphQL schema definition written in [GraphQL SDL](https://blog.graph.cool/graphql-sdl-schema-definition-language-6755bcb9ce51) (= Schema Definition Language), it defines the *structure* of the GraphQL API. The API defined by this schema exposes exactly one query called `description` which returns a simple string.
* `resolvers` is the *implementation* of the API. Notice how the shape of the resolvers object matches the shape of the schema: `Query.description`. The implementation is straightforward, all it does at this point is returning a string with a *description* of the API.
* `typeDefs` and `resolvers` are passed to the constructor of the `GraphQLServer`. Now, whenever the `server` receives the `description` query, it will simply invoke the `Query.description` resolver and respond to the query with the string returned by that resolver.
* Finally, you’re starting the `server`. Note that you’re also passing a callback that’s invoked once the `server` was started —here you simply print a short message to indicate that the server is running on port `4000` (which is the default port of `graphql-yoga`).

So, what happens when you run this thing now?

<Instruction>

Go ahead and try it out:

```bash
node src/index.js
```

</Instruction>

Well, as expected it prints the message in the console. Go ahead and open [http://localhost:4000](http://localhost:4000) inside a browser. What you’ll see is a [GraphQL Playground](https://github.com/graphcool/graphql-playground) — a powerful GraphQL IDE that lets you interactively work with your GraphQL API. Think of it like [Postman](https://www.getpostman.com/), but specifically for GraphQL.

You can now go ahead and send the description query by typing the following into the left editor pane and then hit the **Play**-button in the middle:

```graphql
query {
  description
}
```

Here’s is what you’ll see after you sent the query:

![](https://cdn-images-1.medium.com/max/3898/1*f8w0el_sIlC-s9yX4va88A.png)

Congratulations, you just wrote your first GraphQL server. Easy as pie! 🍰

### Defining an application schema

All right, cool! So, you just learned how to write a GraphQL server with a very basic API. But how does that help for the blogging application we promised you to build? Honest answer: Not too much!

What’s needed is an API that allows to perform certain (domain-specific) operations we'd expect from a blogging app. Let’s lay out a few requirements. The API should allow for the following operations:

* *Create* new *post* items (i.e. blog articles) as *drafts*. A *draft* is a *post* that is not yet *published*. Each *post* should have a *title* and some *content*.
* *Publish* a *draft*.
* *Delete* a *post*, whether it’s *published* or not.
* *Fetch all post* items or *single* one.

Great, that’s four requirements you can directly translate into corresponding API operations.

First, you need to ensure you have a proper *data model* — in this case, that’ll be represented by a single `Post` type. Here is what it looks like written in SDL:

```graphql
type Post {
  id: ID!
  title: String!
  content: String!
  published: Boolean!
}
```

We’ll tell you in a bit where to put that code — bear with us for a minute. Next, you’re going to define the mentioned API operations. That’s done in terms of *queries* and *mutations*. Like with the definition of the `description` query above, you can add fields to the `Query` type as well as to a new `Mutation` type in the GraphQL schema definition:

```graphql
type Query {
  posts: [Post!]!
  post(id: ID!): Post
}

type Mutation {
  createDraft(title: String!, content: String): Post
  deletePost(id: ID!): Post
  publish(id: ID!): Post
}
```

All right, but *where* do you put all that SDL code? Well, in theory you could simply add it to the existing `typeDefs` string in `index.js` since that’s where you define the API for your GraphQL server. However, a cleaner solution is to define the schema in its own file.

<Instruction>

Go ahead and create a new file called `schema.graphql` in the `src` directory:

```bash
touch src/schema.graphql
```

</Instruction>

<Instruction>

`schema.graphql` contains the definition of your **application schema**. Here is what it looks like in its entirety:

```graphql
type Query {
  posts: [Post!]!
  post(id: ID!): Post
  description: String!
}

type Mutation {
  createDraft(title: String!, content: String): Post
  deletePost(id: ID!): Post
  publish(id: ID!): Post
}

type Post {
  id: ID!
  title: String!
  content: String!
  published: Boolean!
}
```

</Instruction>

You simply merged the `Post` type that was defined as your data model together with the API operations — et voilà — there’s your GraphQL schema definition!

There are few things to note about the *types* in that example:

* `Query` and `Mutation` are the so-called [*root types*](http://graphql.org/learn/schema/#the-query-and-mutation-types of your schema. They define the *entry points* for the API.
* `ID`, `String` and `Boolean` are [*scalar types*](http://graphql.org/learn/schema/#scalar-types) that are supported by the official GraphQL type system.
* `Post` is a custom [*object type*](http://graphql.org/learn/schema/#object-types-and-fields) you define inside your schema.
* The `!` following a type means that the corresponding value [can not be `null`](http://graphql.org/learn/schema/#lists-and-non-null). For example, the `posts` query can not return a list where some elements would be `null`. The `post` query on the other hand might return `null` if no `Post` item with the provided `id` exists. Similarly, all mutations might return null in case they fail.

<Instruction>

Also, since you’re now defining the application schema in a separate file, you can delete the `typeDefs` variable from `index.js` and instantiate the `GraphQLServer` like so:

```bash
const server = new GraphQLServer({
  typeDefs: './src/schema.graphql',
  resolvers
})
```

</Instruction>

All right, that’s pretty much all you need to know! So, what’s left to do so your API can actually be used? Correct! Implement the corresponding resolvers.

Each *root field* (i.e. a field on a *root type*) needs to have a backing resolver so the GraphQL engine inside the `GraphQLServer` knows what data to return when a query is requesting that field.

### Implementing resolvers for the application schema

For now, you’ll use a simple _in-memory_ store rather than an actual _persistence_ layer. You’re going add a proper database later!

<Instruction>

Inside `index.js`, go ahead and replace the current implementation of the `resolvers` object with the following:

```js
let idCount = 0
const posts = []

const resolvers = {
  Query: {
    description: () => `This is the API for a simple blogging application`,
    posts: () => posts,
    post: (parent, args) => posts.find(post => post.id === args.id),
  },
  Mutation: {
    createDraft: (parent, args) => {
      const post = {
        id: `post_${idCount++}`,
        title: args.title,
        content: args.content,
        published: false,
      }
      posts.push(post)
      return post
    },
    deletePost: (parent, args) => {
      const postIndex = posts.findIndex(post => post.id === args.id)
      if (postIndex > -1) {
        const deleted = posts.splice(postIndex, 1)
        return deleted[0]
      }
      return null
    },
    publish: (parent, args) => {
      const postIndex = posts.findIndex(post => post.id === args.id)
      posts[postIndex].published = true
      return posts[postIndex]
    },
  },
}
```

</Instruction>

There you go! Each field from the application schema now has a backing resolver:

* `Query.description`: Same as before.
* `Query.posts`: Returns our in-memory array called `posts` where we’re storing all the `Post` items.
* `Query.post`: Searches the `posts` array for a `Post` item with a given `id`. Notice that the `id` is contained inside the `args` argument that’s passed into the resolver. If you want to learn more about the various resolver arguments, check out [this](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e#653a) article.
* `Mutation.createDraft`: Creates a new unpublished `Post` item and appends it to the `posts` array. Again, the arguments are retrieved from the `args` object. They correspond to the arguments defined on the belonging root field!
* `Mutation.deletePost`: Removes the `Post` item with the given `id` from the `posts` array.
* `Mutation.publish`: Sets the `published` property of the `Post` item with the given `id` to `true` and returns it.

Feeling smart yet? 🤓 Go ahead and use this API by starting the server again: `node src/index.js`.

Here’s a few queries and mutations you can play around with, send them one-by-one in the Playground on [http://localhost:4000:](http://localhost:4000:)

```graphql
# 1. Create a new draft
mutation {
  createDraft(
    title: "graphql-yoga is awesome"
    content: "It really is!"
  ) {
    id
    published
  }
}

# 2. Publish the draft
mutation {
  publish(id: "post_0") {
    id
    title
    content
    published
  }
}

# 3. Retrieve all posts
query {
  posts {
    id
    title
    published
  }
}
```

Here’s what the result of the last query will look like after performing the previous mutations:

![](https://cdn-images-1.medium.com/max/3032/1*wcgvOEJkeP2RZ1yZ6ibzSg.png)

Feel free to play around with this API a bit further and explore its capabilities. Of course, no data is actually stored beyond the runtime of the `GraphQLServer`. When you’re stopping and starting the server, all previous `Post` items are deleted.

## Optional: Connecting a database

So, with what we’ve covered by now you should have gotten a feeling for what’s going on in the internals of a GraphQL server. You define a _schema_, implement _resolvers_ and there’s your API!

> **Note**: Resolvers can return data from anywhere — a database, an existing REST API, a 3rd-party service or even another GraphQL API.

The nice thing about resolvers is that they are super flexible, meaning they’re not bound to a particular data source! This allows for example to return data simply from memory as seen in the example, or otherwise fetch data from a database, an existing REST API, a 3rd-party service or even another GraphQL API.

In this example, we’ll connect the resolvers to [Prisma](https://www.prisma.io). Prisma provides a GraphQL API as an abstraction over a database (in this tutorial, this will be a MySQL DB). Thanks to [prisma-binding](https://github.com/graphcool/prisma-binding) (hold on a bit, we’ll talk about this soon), implementing the resolvers merely becomes a question of [*delegating*](https://blog.graph.cool/graphql-schema-stitching-explained-schema-delegation-4c6caf468405) incoming queries to the underlying Prisma API instead of writing complicated SQL yourself.

Think of Prisma as an [ORM-like layer](https://github.com/prisma/prisma#is-prisma-an-orm) for your GraphQL server.

### Adding a Prisma configuration to the project

To introduce Prisma into the project, you need to to make a few rearrangements.

<Instruction>

First, you need to update the application schema in `schema.graphql` so that it looks as follows:

```graphql
# import Post from "./generated/prisma.graphql"

type Query {
  posts: [Post!]!
  post(id: ID!): Post
  description: String!
}

type Mutation {
  createDraft(title: String!, content: String): Post
  deletePost(id: ID!): Post
  publish(id: ID!): Post
}
```

</Instruction>

Wait what? You’re removing the `Post` type and instead _import_ it (using a GraphQL _comment_?!) on top of the file from a file that doesn’t even exist? 😠

Well, yes! The comment syntax for importing stems from the [`graphql-import`](https://github.com/graphcool/graphql-import) library and is not part of the official spec ([yet!](https://github.com/graphql/graphql-wg/blob/master/notes/2018-02-01.md#present-graphql-import)). You’ll take care of creating the `generated/prisma.graphql` file in a bit!

Next, you'll setup your Prisma database service.

<Instruction>

Therefore, you first need to install the [Prisma CLI](!alias-foatho8aip):

```bash
npm install -g prisma
```

</Instruction>

<Instruction>

Once installed, you can use the `prisma init` command to create a new directory which will contain the configuration files for your Prisma database service:

```bash
prisma init database
```

</Instruction>

<Instruction>

When prompted by the CLI whether you want to create a new [Prisma server](!alias-eu2ood0she) or deploy an existing one, select the **Demo server** and hit **Enter**.

Note that this requires you to authenticate with [Prisma Cloud](https://www.prisma.io/cloud/) as that's where the Prisma server is hosted.

</Instruction>

> **Note**: If you have Docker installed, you can also deploy to a Prisma server that's running locally. To do so, you can choose the **Create new database** option in the prompt.

<Instruction>

The CLI further prompts you to select a _region_ to which the Prisma service should be deployed as well as a _name_ and a _stage_ for the service. You can just select the suggested values by hitting **Enter**.

</Instruction>

All `prisma init` is doing here is creating a new directory called `database` and places two files in there:

- [`prisma.yml`](!alias-foatho8aip): The root configuration file for your Prisma service.
- [`datamodel.graphql`](!alias-eiroozae8u): Contains the definition of your data model in SDL (Prisma will translate this into an according database schema).

Your generated `prisma.yml` looks similar to this:

```yml
endpoint: https://eu1.prisma.sh/jane-doe/database/dev
datamodel: datamodel.graphql
```

> **Note**: The `jane-doe` part of the `endpoint` will be different in your case as that's the identifier for your personal workspace in Prisma Cloud. If you have deployed the service locally with Docker,there is no such workspace ID.

<Instruction>

Next, update the contents of `datamodel.graphql` to look as follows:

```graphql
type Post {
  id: ID! @unique
  title: String!
  content: String!
  published: Boolean! @default(value: "false")
}
```

</Instruction>

The definition is identical to the `Post` from before, except that you’re adding these `@default` and `@unique` directives. The `@unique` directive enforces that no two `Post` items can have the same value for the `id` field; the `@default` directive means that each `Post` item that will be stored with Prisma will have the value for this field set to `false` if not otherwise specified. And the best thing is, Prisma is taking care of that without us needing to do anything else. Neat! 🙌

### Deploying the Prisma service

Ok cool! So, what did we win now? So far, not much! But let’s go ahead and deploy the Prisma service and see what we can do then.

<Instruction>

Navigate into the `database` directory and execute `prisma deploy`:

```bash
cd database
prisma deploy
```

</Instruction>

After the command has finished, it prints an HTTP endpoint that you can open in a browser. This is the same endpoint that's already specified in `prisma.yml`.

When you’re opening the URL with a browser, you’ll see the Playground for the Prisma API. This API defines CRUD operations for the `Post` type that’s defined in your data model. For example, you can send the following queries and mutations:

```graphql
# 1. Create a new Post
mutation {
  createPost(data: {
    title: "graphql-yoga is awesome"
    content: "It really is!"
  }) {
    id
    published
  }
}

# 2. Update title and content of an existing post
mutation {
  updatePost(
    where: {
      id: "__POST_ID__"
    }
    data: {
      title: "New title"
      content: "New content"
    }
  ) {
    id
    title
    published
  }
}


# 3. Retrieve all posts
query {
  posts {
    id
    title
    content
    published
  }
}
```

You now have a GraphQL API that mirrors the CRUD operations of the underlying database. But how does that help with your GraphQL server and implementing the resolvers for the application schema?

Meet [GraphQL bindings](https://github.com/graphql-binding/graphql-binding)!

### Implementing resolvers with Prisma bindings

[GraphQL bindings](https://www.prisma.io/blog/graphql-binding-2-0-improved-api-schema-transforms-automatic-codegen-5934cd039db1/) are a way to easily reuse and share existing GraphQL APIs. Each GraphQL API is represented as a JavaScript object that exposes a number of methods. Each method corresponds to a query or mutation — but instead of having to spell out the entire query or mutation as a string, you can invoke the corresponding method and the binding object will construct and send the query under the hood.

This is particularly nice for typed languages, where you then get compile-time error checks as well as auto-completion features for GraphQL operations! 💯

<Instruction>

The first step to introduce bindings is to add the corresponding dependency to your project (the following command needs to be executed inside the `blogr`, not the `database` directory):

```bash
yarn add prisma-binding
```

</Instruction>

<Instruction>

Now, you can update the implementation of `index.js` as follows:

```js
const { GraphQLServer } = require('graphql-yoga')
const { Prisma } = require('prisma-binding')

const resolvers = {
  Query: {
    posts(parent, args, ctx, info) {
      return ctx.db.query.posts({ }, info)
    },
    post(parent, args, ctx, info) {
      return ctx.db.query.post({ where: { id: args.id } }, info)
    },
  },
  Mutation: {
    createDraft(parent, { title, content }, ctx, info) {
      return ctx.db.mutation.createPost(
        {
          data: {
            title,
            content,
          },
        },
        info,
      )
    },
    deletePost(parent, { id }, ctx, info) {
      return ctx.db.mutation.deletePost({ where: { id } }, info)
    },
    publish(parent, { id }, ctx, info) {
      return ctx.db.mutation.updatePost(
        {
          where: { id },
          data: { published: true },
        },
        info,
      )
    },
  },
}

const server = new GraphQLServer({
  typeDefs: './src/schema.graphql',
  resolvers,
  context: req => ({
    ...req,
    db: new Prisma({
      typeDefs: 'src/generated/prisma.graphql', // the generated Prisma DB schema
      endpoint: '__PRISMA_ENDPOINT__',          // the endpoint of the Prisma DB service
      secret: 'mysecret123',                    // specified in database/prisma.yml
      debug: true,                              // log all GraphQL queries & mutations
    }),
  }),
})

server.start(() => console.log('Server is running on http://localhost:4000'))
```

</Instruction>

Note that you need to replace the __PRISMA_ENDPOINT__ placeholder in **line 47** with the `endpoint` of your Prisma service (the one specified in `prisma.yml`).

In each resolver, you’re now basically *forwarding* the incoming request to the underlying Prisma API with its powerful query engine. Explaining what’s going on there in detail is beyond the scope of this tutorial — but if you want to learn more, check out these articles:

* [**Reusing & Composing GraphQL APIs with GraphQL Bindings**](https://blog.graph.cool/reusing-composing-graphql-apis-with-graphql-bindings-80a4aa37cff5)
* [**GraphQL Server Basics: Demystifying the `info` Argument in GraphQL Resolvers**](https://blog.graph.cool/graphql-server-basics-demystifying-the-info-argument-in-graphql-resolvers-6f26249f613a)

In any case, you can see that the implementation of the resolvers is almost trivial and the hard work is done by the underlying Prisma service. And now just imagine you’d have to write SQL queries in these resolvers… 😱

### Generating the Prisma database schema

There is one missing piece before you can start the server again, and that is the dubious `generated/prisma.graphql` file.

The workflow for getting a hold of this file is based the [GraphQL CLI](https://github.com/graphql-cli/graphql-cli) as well as on [`graphql-config`](https://github.com/graphcool/graphql-config) (a configuration standard for GraphQL projects). To get this up-and-running, first need to installed the GraphQL CLI and then create a `.graphqlconfig` file.

<Instruction>

Run the following command in your terminal to install the GraphQL CLI:

```bash
npm install -g graphql-cli
```

</Instruction>

<Instruction>

Next, create you `.graphqlconfig` file inside the `blogr` directory:

```bash
touch .graphqlconfig.yml
```

</Instruction>

<Instruction>

Then, add the following contents to it:

```yml
projects:
  database:
    schemaPath: src/generated/prisma.graphql
    extensions:
      prisma: database/prisma.yml
  app:
    schemaPath: src/schema.graphql
    extensions:
      endpoints:
        default: http://localhost:4000
```

</Instruction>

Notice that you’re also adding information about your local `graphql-yoga` server — not only the Prisma API!

<Instruction>

Now, to generate the `prisma.graphql` file (also called **Prisma database schema**), all you need to do is run `get-schema` command from the GraphQL CLI:

```bash
graphql get-schema
```

</Instruction>

Because the GraphQL CLI “understands” the `.graphqlconfig.yml`, it knows that it should download the schema from the `endpoint` specified in `prisma.yml` and put the generated GraphQL schema definition into `src/generated/prisma.graphql`, pretty smart huh? 😎

<InfoBox>

💡 **Pro tip**: To ensure your Prisma database schema is always in sync with the deployed API, you can also add a `post-deploy` [hook](!alias-ufeshusai8#hooks-optional) to your `prisma.yml` file. Whenever you're updating the data model (and therefore the Prisma database schema) by running `prisma deploy`, the CLI will automatically download the schema for the updated API.
<br>
To do so, add the following code to the end of `prisma.yml`:

```yml
hooks:
  post-deploy:
    - graphql get-schema
```

</InfoBox>

### Testing the app

All right, everything is in place now! You can finally start the GraphQL server again: `node src/index.js`

Because the application schema hasn’t changed (only its *implementation* was updated), you can send the same queries and mutations from before to test your API. Of course, now the data you're storing will be persisted in the database that's proxied by Prisma.

Also, here’s a little gem: If you [download the standalone version of the GraphQL Playground](https://github.com/graphcool/graphql-playground/releases/), you can work with the API of your `graphql-yoga` server and the Prisma API side-by-side (run `prisma playground` after you downloaded and installed it on your machine). The projects are read from the `.graphqlconfig.yml` file as well:

![](https://cdn-images-1.medium.com/max/2764/1*OZy6mDfz8vZIrmvP9RvSHg.png)

## Where to go from here?

In this post, you learned how to build a GraphQL server from scratch. In the end, you had a working API for a blogging application where posts would be stored in an actual database.

Along the way you learned about important concepts, such as [GraphQL schemas](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e), resolver functions, [GraphQL bindings](https://blog.graph.cool/reusing-composing-graphql-apis-with-graphql-bindings-80a4aa37cff5), [`graphql-config`](https://github.com/graphcool/graphql-config) and a lot more!

Also, we didn’t tell you this before but with this tutorial you basically rebuilt the [`node-basic`](https://github.com/graphql-boilerplates/node-graphql-server/tree/master/basic) GraphQL [boilerplate](https://blog.graph.cool/graphql-boilerplates-graphql-create-how-to-setup-a-graphql-project-6428be2f3a5) project.

If you want to go one step further and learn about how you can implement authentication and realtime functionality with GraphQL subscriptions, you can check out the fully-fledged [Node tutorial on How to GraphQL](https://www.howtographql.com/graphql-js/0-introduction/). To deploy your GraphQL server, check out this tutorial:
[Deploying GraphQL Servers with Zeit Now](https://blog.graph.cool/deploying-graphql-servers-with-zeit-now-85f4757b79a7).

In case you got lost at some point during the tutorial, you can check out the working version of the final project [here](https://github.com/nikolasburk/blogr).

Namaste 🙏
