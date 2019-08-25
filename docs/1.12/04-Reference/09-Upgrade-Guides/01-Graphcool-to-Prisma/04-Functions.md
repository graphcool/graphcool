---
alias: cheechoo8o
description: Functions
---

# Functions

In this section, you'll learn how you can migrate the functionality of your serverless [functions](https://www.graph.cool/docs/reference/functions/overview-aiw4aimie9) from the Graphcool Framework to Prisma. This includes hooks, resolvers and server-side subscriptions.

## Hooks

Hooks in the Graphcool Framework are used for synchronous data validation and transformation. You can associate a hook with a certain mutation of your GraphQL API. Before the mutation is performed, the Graphcool Function executes the hook function which can either transform the incoming mutation arguments or throw an error if a validation rule is violated.

Assume this data model for the following example:

```graphql
type User {
  id: ID! @unique
  name: String!
}
```

Further, assume your GraphQL server exposes the following _application schema_:

```graphql
type Query {
  users: [User!]!
}

type Mutation {
  createUser(name: String!): User!
}
```

### Data validation

The example we'll use for data validation is that we don't want to allow `User` nodes where the `name` has fewer than two letters.

#### Graphcool Framework

In a hook function, we'd ensure this constraint by implementing the following function and associating it with the `createUser` mutation of the Graphcool Framework's GraphQL API:

```js
event => {

  if (event.data.name.length < 2) {
    return { error: `The provided name '${event.data.name}' is too short. A name must have at least two letters.` }
  }

  return { data: event.data }
}
```

#### Prisma

Now, with Prisma that functionality moves into the application layer, i.e. the implementation of your GraphQL server. More precisely, the check needs to be performed _inside_ the `createUser` resolver:

```js
function createUser(parent, { name }, context, info) {
  if (name.length < 2) {
    throw new Error(`The provided name '${name}' is too short. A name must have at least two letters.`)
  }

  return context.db.mutation.createUser({ data: { name }, info)
}
```

### Data transformation

For data transformation, the idea is similar. Again, the functionality that was previously implemented in a hook function now moves into your application layer.

As an example for data transformation we're implementing the use case that we only want to store the value for `name` fields completely uppercased.

##### Graphcool Framework

```js
event => {
  const uppercaseName = event.data.name.toUppercase()
  return { data: uppercaseName }
}
```

##### Prisma

As mentioned, Prisma solves the same issue inside the `createUser` resolver:

```js
function createUser(parent, { name }, context, info) {

  const uppercaseName = name.toUppercase()
  return context.db.mutation.createUser({ data: { name: uppercaseName }, info)
}
```

## Resolver Functions

Resolver functions in the Graphcool Framework are used to extend the capabilities of the auto-generated CRUD API. Typical use cases include authentication (e.g. `signup` and `login` mutations) as well as integrating 3rd party services or wrapping REST APIs.

For the following example, we'll consider the use case of wrapping a REST API (based on the [`rest-wrapper`](https://github.com/graphcool/graphcool-framework/tree/master/examples/0.x/rest-wrapper) example from the Graphcool Framework). We'll use `https://dog.ceo/api/breed/${breedName}/images/random` endpoint.

### Graphcool Framework

With the Graphcool Framework, there are two parts to a resolver function:

- a **schema extension** written in SDL
- the **resolver implementation** in JavaScript

The schema extension needs to extend the `Query` type and define a new root field that can be used by clients to submit a query:

```graphql
type RandomBreedImagePayload {
  url: String!
}

extend type Query {
  randomBreedImage(breedName: String!): RandomBreedImagePayload!
}
```

The implementation of the resolver then retrieves the `breedName` argument from the incoming `event` and makes the call to the mentioned REST endpoint. It also needs to ensure the returned data has the right structure, i.e. it needs to adhere to the defined `RandomBreedImagePayload` type.

```js
require('cross-fetch/polyfill')

module.exports = event => {

  const { breedName } = event.data
  const url = `https://dog.ceo/api/breed/${breedName}/images/random`

  return fetch(url)
    .then(response => response.json())
    .then(responseData => {
      const randomBreedImageData = responseData.message
      const randomBreedImage = { url: randomBreedImageData }
      return { data: randomBreedImage }
    })
}
```

### Prisma

Similar as for hooks, the functionality of Graphcool Framework resolver functions is now implemented in the application layer and thus not directly taken care of by Prisma any more.

The application schema needs to define the corresponding root field:

```graphl
type RandomBreedImagePayload {
  url: String!
}

extend type Query {
  randomBreedImage(breedName: String!): RandomBreedImagePayload!
}
```

The resolver is then simply part of your GraphQL server implementation:

```js
function(parent, { breedName }, context, info) {
  const url = `https://dog.ceo/api/breed/${breedName}/images/random`

  return fetch(url)
    .then(response => response.json())
    .then(responseData => {
      const randomBreedImageData = responseData.message
      const randomBreedImage = { url: randomBreedImageData }
      return { data: randomBreedImage }
    })
}
```

## Subscriptions

Server-side subscriptions are following the same concept in Prisma. However, with Prisma it is not possible to host the corresponding functions as [managed functions](https://www.graph.cool/docs/reference/functions/overview-aiw4aimie9#managed-functions-vs-webhooks) anymore - instead they need to be configured through _web hooks_, pointing to an HTTP endpoint that you deployed yourself (e.g. using AWS Lamba, Google Cloud Functions, Zeit Now, ...).

The following example is based on the [`subscriptions`](https://github.com/graphcool/graphcool-framework/blob/master/examples/0.x/subscriptions) from the Graphcool Framework.

### Graphcool Framework

To configure a server-side subscription in the Graphcool Framework, you need to provide two components:

- a **GraphQL subscription query** that defines what event you're subscribing for and what data you'd like to receive when the event is happening
- a **handler** that will be invoked when the event is happening - this can either be a managed function or a webhook

With the following subscription query, we express that we want to invoke the handler when a new `User` node is _created_. The event payload should carry the `id` and `name` of the newly created `User`.

**`createFirstArticle.graphql`**

```graphql
subscription {
  User(filter: {
    mutation_in: [CREATED]
  }) {
    node {
      id
      name
    }
  }
}
```

We now specify the handler function as a managed function.

**`createFirstArticle.js`**

```js
const { fromEvent } = require('graphcool-lib')

module.exports = event => {

  // Retrieve payload from event
  const { id, name } = event.data.User.node

  // Create Graphcool API (based on https://github.com/graphcool/graphql-request)
  const graphcool = fromEvent(event)
  const api = graphcool.api('simple/v1')

  // Create variables for mutation
  const title = `My name is ${name}, and this is my first article!`
  const variables = { authorId: id, title }

  // Create mutation
  const createArticleMutation = `
    mutation ($title: String!, $authorId: ID!) {
      createArticle(title: $title, authorId: $authorId) {
        id
      }
    }
  `

  // Send mutation with variables
  return api.request(createArticleMutation, variables)
}
```

Notice that the incoming `event` has the following structure (adhering to the shape of the subscription query):

```json
{
  "data": {
    "User": {
      "node": {
        "id": "cj8wscby6nl7u0133zu7c8a62",
        "name": "Sarah"
      }
    }
  }
}
```

Notice that the managed function is configured in `graphcool.yml`, pointing to the subscription function and the implementation of the managed function:

```yml
functions:
  createFirstArticle:
    type: subscription
    query: src/createFirstArticle.graphql
    handler:
      code: src/createFirstArticle.js
```

### Prisma

With Prisma, you still configure the subscription in your service's root configuration file. However, the YAML keys are a bit different and you can only point to the handler as a webhook:

```yml
subscriptions:
  createFirstArticle:
    query: src/createFirstArticle.graphql
    webhook: https://bcdeaxokbj.execute-api.eu-west-1.amazonaws.com/dev/createFirstArticle
```

This example assumes that you have deployed a serverless function to the endpoint `https://bcdeaxokbj.execute-api.eu-west-1.amazonaws.com/dev/createFirstArticle`.
