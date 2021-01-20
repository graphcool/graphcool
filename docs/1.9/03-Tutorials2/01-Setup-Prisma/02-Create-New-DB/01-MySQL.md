---
alias: gui4peul2u
description: Learn how to setup a Prisma server by creating a new MySQL database.
---

# Setup a Prisma server by creating a new MySQL database

In this tutorial, you will learn how to get started with Prisma. You'll create a new Prisma server (using Docker) and deploy your first Prisma service to it.

<InfoBox>

To ensure you're not accidentally skipping an instruction in the tutorial, all required actions are highlighted with a little _counter_ on the left.

**Pro tip**: If you're only keen on getting started but don't care so much about the explanations of what's going on, you can simply jump from instruction to instruction.

</InfoBox>

## Step 1: Install Docker

This tutorial teaches you how to build a Prisma server that's running locally on your machine. The server is based on [Docker](https://www.docker.com), be sure to have it installed before moving on with the tutorial.

<Instruction>

If you don't have Docker installed already, you can download it for your platform using the following links:

- [Mac](https://store.docker.com/editions/community/docker-ce-desktop-mac)
- [Windows](https://store.docker.com/editions/community/docker-ce-desktop-windows)
- [Other](https://www.docker.com/get-docker)

</Instruction>

<Instruction>

Once the installer was downloaded, double-click on it and follow the instructions for the installation on your platform.

</Instruction>

## Step 2: Install the Prisma CLI

Prisma services are managed with the [Prisma CLI](!alias-je3ahghip5). You can install it using `npm` (or `yarn`).

<Instruction>

Open your terminal and run the following command to install the Prisma CLI:

```
npm install -g prisma
# or
# yarn global add prisma
```

</Instruction>

## Step 3: Create your Prisma server

In this tutorial, you'll create all the files for your Prisma setup manually.

<Instruction>

Open a terminal and navigate to a folder of your choice and create a new directory to store the files for your Prisma project:

```sh
mkdir hello-world
```

</Instruction>

<Instruction>

Next, navigate into the new directory and create the Docker compose file that specifies the Docker images for your Prisma server and its MySQL database:

```sh
cd hello-world
touch docker-compose.yml
```

</Instruction>

<Instruction>

Now paste the following contents into it:

```yml
version: '3'
services:
  prisma:
    image: prismagraphql/prisma:1.8
    restart: always
    ports:
    - "4466:4466"
    environment:
      PRISMA_CONFIG: |
        port: 4466
        # uncomment the next line and provide the env var PRISMA_MANAGEMENT_API_SECRET=my-secret to activate cluster security
        # managementApiSecret: my-secret
        databases:
          default:
            connector: mysql
            host: mysql
            port: 3306
            user: root
            password: prisma
            migrations: true
  mysql:
    image: mysql:5.7
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: prisma
    volumes:
      - mysql:/var/lib/mysql
volumes:
  mysql: ~
```

</Instruction>

To learn more about the structure of this Docker compose file, check out the [reference documentation](!alias-aira9zama5#configuration-with-docker-compose).

<Instruction>

With the Docker compose file in place, go ahead and start the Docker containers using the [`docker-compose`](https://docs.docker.com/compose/reference/overview/) CLI:

```sh
docker-compose up -d
```

</Instruction>

Your Prisma server is now running on `http://localhost:4466` which means you can now start deploying Prisma services to it using the Prisma CLI.

## Step 4: Create your Prisma service

The minimal setup you need for creating a Prisma service consists of two files:

- [`prisma.yml`](!alias-foatho8aip): The root configuration file for your service.
- `datamodel.graphql` (can also be called differently, e.g. `types.graphql`): This file contains the definition of your [data model](!alias-eiroozae8u) (written in [GraphQL SDL](https://blog.graph.cool/graphql-sdl-schema-definition-language-6755bcb9ce51)).

<Instruction>

Create both files by running the following commands in your terminal:

```sh
touch prisma.yml
touch datamodel.graphql
```

</Instruction>

<Instruction>

Next, open `prisma.yml` and paste the following contents into it:

```sh
endpoint: http://localhost:4466
datamodel: datamodel.graphql
```

</Instruction>

<Instruction>

To complete the setup, open `datamodel.graphql` and add the following `User` type to it:

```graphql
type User{
  id: ID! @unique
  name: String!
}
```

</Instruction>

The `@unique` directive here expresses that no two `User` records in the database can have the same `id`. Prisma will ensure this requirement is met at all times.

## Step 5: Deploy your Prisma service to a Demo server

So far you only have the local service configuration files for your Prisma service available, but you haven't deployed anything to the Prisma server that's running on your machine yet.

<Instruction>

Run the following command to deploy the Prisma service to your local Prisma server:

```sh
prisma deploy
```

</Instruction>

Your Prisma API is now deployed and ready to receive your queries, mutations and subscriptions 🎉

## Step 6: Explore Prisma's GraphQL API in a GraphQL Playground

So your Prisma server is deployed - but how do you know how to interact with it? What does its API actually look like?

In general, the generated API allows to perform CRUD operations on the types in your data model. It also exposes GraphQL subscriptions which allow clients to _subscribe_ to certain _events_ and receive updates in realtime.

It is important to understand that the data model is the foundation for your API. Every time you make changes to your data model (and run `prisma deploy` afterwards), the schema of Prisma's GraphQL API gets updated accordingly.

Because your data model contains the `User` type, the Prisma API now allows for its clients to create, read, update and delete records, also called _nodes_, of that type. In particular, the following GraphQL operations are now generated based on the `User` type:

- `user`: Query to retrieve a single `User` node by its `id` (or another `@unique` field).
- `users`: Query to retrieve a list of `User` nodes.
- `createUser`: Mutation to create a new `User` node.
- `updateUser`: Mutation to update an existing `User` node.
- `deleteUser`: Mutation to delete an existing `User` node.

> **Note**: This list of generated operations is not complete. The Prisma API exposes a couple of more operations that, for example, allow to batch update/delete many nodes. However, all operations either create, read, update or delete nodes of the types defined in the data model.

To actually use these operations, you need a way to [send requests to your service's API](!alias-ohm2ouceuj). Since that API is exposed via HTTP, you could use tools like [`curl`](https://en.wikipedia.org/wiki/CURL) or [Postman](https://www.getpostman.com/) to interact with it. However, GraphQL actually has much nicer tooling for that purpose: [GraphQL Playground](https://github.com/graphcool/graphql-playground), an interactive GraphQL IDE.

<Instruction>

To open a GraphQL Playground you can use the Prisma CLI again. Run the following command inside the `hello-world` directory:

```sh
prisma playground
```

</Instruction>

This will open a Playground looking as follows:

![](https://imgur.com/HuJfglj.png)

> **Note**: The Playground can be installed on your machine as a [standalone desktop application](https://github.com/graphcool/graphql-playground/releases). If you don't have the Playground installed, the command automatically opens a Playground in your default browser.

One really cool property of GraphQL APIs is that they're effectively _self-documenting_. The [GraphQL schema](https://blog.graph.cool/graphql-server-basics-the-schema-ac5e2950214e) defines all the operations of an API, including input arguments and return types. This allows for tooling like the GraphQL Playground to auto-generate API documentation.

<Instruction>

To see the documentation for your Prisma API, click the green **SCHEMA**-button on the right edge of the Playground window.

</Instruction>

This brings up the Playground's documentation pane. The left-most column is a list of all the operations the API accepts. You can then drill down to learn the details about the input arguments or return types that are involved with each operation.

![](https://imgur.com/l82HjFR.png)

## Step 7: Send queries and mutations

All right! With everything you learned so far, you're ready to fire off some queries and mutations against your API. Let's start with the `users` query to retrieve all the `User` nodes currently stored in the database.

<Instruction>

Enter the following query into the left Playground pane and click the **Play**-button (or use the hotkey **CMD+Enter**):

```graphql
query {
  users {
    name
  }
}
```

</Instruction>

At this point, the server only returns an empty list. This is no surprise as we haven't actually created any `User` nodes so far. So, let's change that and use the `createUser` mutation to store a first `User` node in the database.

<Instruction>

Open a new tab in the Playground, enter the following mutation into the left Playground pane and send it:

```graphql
mutation {
  createUser(data: {
    name: "Sarah"
  }) {
    id
  }
}
```

</Instruction>

This time, the response from the server actually contains some data (note that the `id` will of course vary as the server generates a globally unique ID for every new node upon creation):

```json
{
  "data": {
    "createUser": {
      "id": "cjc69nckk31jx01505vgwmgch"
    }
  }
}
```

<Instruction>

You can now go back to the previous tab with the `users` query and send that one again.

</Instruction>

This time, the `User` node that was just created is returned in the server response:

![](https://imgur.com/LfXtmbc.png)

Note that the API also offers powerful filtering, ordering and pagination capabilities. Here are examples for queries that provide the corresponding input arguments to the `users` query.

**Retrieve all `User` nodes where the `name` contains the string `"ra"`**

```graphql
query {
  users(where: {
    name_contains: "ra"
  }) {
    id
    name
  }
}
```

**Retrieve all `User` nodes sorted descending by their names**

```graphql
query {
  users(orderBy: name_DESC) {
    id
    name
  }
}
```

**Retrieve a chunk of `User` nodes (position 20-29 in the list)**

```graphql
query {
  users(skip: 20, first: 10) {
    id
    name
  }
}
```

## Where to go from here?

You can now go and **[build a GraphQL server](!alias-ohdaiyoo6c)** based on this API.
