---
alias: fbgaeyrou7
description: Connect to Prisma from the frontend
---

# Connect to Prisma from the frontend

This tutorial teaches you how to communicate with your Prisma service from a Javascript client. You will use React to develop your UI and Apollo Boost to instantiate a GraphQL client. This setup is perfect for learning or prototyping, but it is not a production setup.

<InfoBox type=warning>

⚠️ **Warning**: You should only access your Prisma service from a frontend for prototyping or learning purposes. Please don't access your Prisma service from a production client side application.

Prisma turns your database into a GraphQL API, exposing powerful CRUD operations to read and modify the data. This means letting your clients talk to Prisma directly is equivalent to directly exposing your entire database to your clients.

There are several reasons why this is not a suitable setup for production use cases:

* Your clients should be able to consume a domain-specific API rather than working with generic CRUD operations
* You want to provide authentication functionality for your users so that they can register with a password or some 3rd-party authentication provider
* You want your API to integrate with microservices or other legacy systems
* You want to include 3-rd party services (such as Stripe, GitHub, Yelp, ...) or other public APIs into your server
* You don't want to expose your entire database schema the everyone (which would be the case due to GrapHQL's introspection feature)

</InfoBox>

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

## Step 2: Bootstrap frontend

Now that you have updated the data model on your Prisma service, it's time to start working on the frontend. In this step you will setup a mini blog application written in React.

This is what your directory will look like after you create all folders and files.

```
.
├── package.json
├── prisma
│   ├── datamodel.graphql
│   └── prisma.yml
├── public
│   └── index.html
├── src
│   └── index.js
└── yarn.lock
```

<Instruction>

Run the following commands to create all the files you need to start bootstrap your frontend.

```bash
yarn init -y
mkdir public src prisma
touch public/index.html src/index.js
mv datamodel.graphql prisma.yml prisma
```

Here is a rundown of the commands you just ran.

* `yarn init -y` creates a file called `package.json` with default values
* `mkdir public src prisma` creates three folders called `public`, `src`, and `prisma`
* `touch public/index.html src/index.js` creates two files called `public/index.html` and `src/index.js`
* `mv datamodel.graphql prisma.yml prisma` moves your Prisma configuration file to the `prisma` folder

</Instruction>

Install the dependencies that the initial version of the frontend will need. The first dependency, `react-scripts`, will provide you commands to start, build and test the application. The other dependencies will allow your frontend to render a Blog written in React.

<Instruction>

Use yarn to install the dependencies that your app will need.

```bash
yarn add react-scripts --dev
yarn add react react-dom blog-components
```

</Instruction>

Every frontend app needs an html file, so let's go ahead and create one.

<Instruction>

Paste the following code into `public/index.html`.

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta name="theme-color" content="#000000">
    <title>React App</title>
  </head>
  <body>
    <noscript>
      You need to enable JavaScript to run this app.
    </noscript>
    <div id="root"></div>
  </body>
</html>
```

</Instruction>

That's it for the boilerplate that your client needs. From now on you will focus exclusively on `src/index.js`.

You are going to import a couple of components from `blog-components`. Each of them render a different page, and receive state and/or functions. The main `App` component is going to keep track of the current user and list of posts in its local state. `App` will pass down those values to `CreateUser`, `NewPost` and `Posts`.

The role of `CreateUser` is rendering a user creation page in the root URL (`/`). It receives a prop called `createUser`, which will receive the name that the user entered in a text input.

Once the visitor creates a user, he/she will be redirected to `/new-post`. This URL renders the `NewPost` component. This component presents a form with two text inputs, one for the post title and another for the post content. It receives two props, `user` and `createPost`.

After creating a new post, the visitor will see the post list in `/posts`. The only responsibility of `Posts` is rendering a list of posts.

<Instruction>

Fill `src/index.js` with the contents of the following snippet.

```js
import React from "react";
import ReactDOM from "react-dom";
import { Container, CreateUser, Posts, NewPost } from "blog-components";

class App extends React.Component {
  state = { user: null, posts: [] };
  createUser = name => {
    this.setState({ user: { name } });
  };
  createPost = ({ title, content }) => {
    this.setState({
      posts: [...this.state.posts, { title, content, author: this.state.user }]
    });
  };
  render() {
    return (
      <Container user={this.state.user}>
        <CreateUser createUser={this.createUser} />
        <NewPost user={this.state.user} createPost={this.createPost} />
        <Posts posts={this.state.posts} />
      </Container>
    );
  }
}

ReactDOM.render(<App />, document.getElementById("root"));
```

</Instruction>

It's time to see what this blog app looks like at this point.

<Instruction>

Start a development version of the app with the next command.

```bash
`npx react-scripts start`
```

</Instruction>

This was an intermediate step. The final app will store state in your Prisma service. In the rest of the tutorial you are going to setup a GraphQL client which will send queries and mutations to your Prisma database.

## Step 3: Setup GraphQL client

In this step you will create a GraphQL client and point it to your Prisma URL. The GraphQL client you are going to use is Apollo Boost. It lets you collocate GraphQL queries and mutations alongside React components.

<Instruction>

Install Apollo Boost, React Apollo and `graphql`.

```bash
yarn add apollo-boost react-apollo graphql
```

</Instruction>

<Instruction>

Instantiate a new instance of Apollo Client and point it to your Prisma URL. Wrap your components with `react-apollo`'s `ApolloProvider`. This component receives an instance of Apollo Client and allows any component in your React hierarchy to easily send queries and mutations.

Please note that you need to replace the `__YOUR_PRISMA_ENDPOINT__` placeholder with the endpoint of your Prisma API (which you can find in `prisma.yml`).

Modify `src/index.js` to look like the following:

```js
import React from "react";
import ReactDOM from "react-dom";
import { Container, CreateUser, Posts, NewPost } from "blog-components";
import { ApolloProvider } from "react-apollo";
import ApolloClient from "apollo-boost";

const client = new ApolloClient({
  uri: "__YOUR_PRISMA_ENDPOINT__"
});

class App extends React.Component {
  state = { user: null, posts: [] };
  createUser = name => {
    this.setState({ user: { name } });
  };
  createPost = ({ title, content }) => {
    this.setState({
      posts: [...this.state.posts, { title, content, author: this.state.user }]
    });
  };
  render() {
    return (
      <ApolloProvider client={client}>
        <Container user={this.state.user}>
          <CreateUser createUser={this.createUser} />
          <NewPost user={this.state.user} createPost={this.createPost} />
          <Posts posts={this.state.posts} />
        </Container>
      </ApolloProvider>
    );
  }
}

ReactDOM.render(<App />, document.getElementById("root"));
```

</Instruction>

## Step 4: Send queries and mutations to your Prisma service

In this final step, you will manage state in your Prisma database instead of in `App`'s local state. You will achieve this using React Apollo's `Query` and `Mutation` components. These components receive a GraphQL query and return information to their children. This information can be a GraphQL response in the case of `Query`, or a function which will generate a mutation request to a server in the case of `Mutation`.

Both `Mutation` and `Query` receive a GraphQL mutation parsed with `graphql-tag`. You will create two mutations, `CREATE_USER` and `CREATE_POST` and a query, `POSTS`. React Apollo will send these operations to your Prisma service.

<Instruction>

Wrap `CreateUser`, `Posts` and `NewPost` with their respective `Query` or `Mutation` components.

```js
import React from "react";
import ReactDOM from "react-dom";
import { Container, CreateUser, Posts, NewPost } from "blog-components";
import { ApolloProvider, Query, Mutation } from "react-apollo";
import ApolloClient from "apollo-boost";
import gql from "graphql-tag";

const client = new ApolloClient({
  uri: "__YOUR_PRISMA_ENDPOINT__"
});

const POSTS = gql`
  {
    posts {
      title
      content
      author {
        name
      }
    }
  }
`;

const CREATE_USER = gql`
  mutation CreateUser($name: String!) {
    createUser(data: { name: $name }) {
      id
      name
    }
  }
`;

const CREATE_POST = gql`
  mutation CreatePost($post: PostCreateInput!) {
    createPost(data: $post) {
      id
      title
      content
    }
  }
`;

class App extends React.Component {
  state = { user: null };
  render() {
    return (
      <ApolloProvider client={client}>
        <Container user={this.state.user}>
          <Mutation
            mutation={CREATE_USER}
            update={(cache, { data }) => {
              this.setState({ user: data.createUser });
            }}
          >
            {createUser => (
              <CreateUser
                createUser={name => createUser({ variables: { name } })}
              />
            )}
          </Mutation>
          <Mutation mutation={CREATE_POST}>
            {createPost => (
              <NewPost
                user={this.state.user}
                createPost={({ title, content }) =>
                  createPost({
                    variables: {
                      post: {
                        title,
                        content,
                        author: { connect: { id: this.state.user.id } }
                      }
                    },
                    refetchQueries: [{ query: POSTS }]
                  })
                }
              />
            )}
          </Mutation>
          <Query query={POSTS}>
            {({ loading, error, data }) => {
              if (error) {
                return <div>Error :(</div>;
              }
              if (loading) {
                return <div>Loading...</div>;
              }
              return <Posts posts={data.posts} />;
            }}
          </Query>
        </Container>
      </ApolloProvider>
    );
  }
}

ReactDOM.render(<App />, document.getElementById("root"));
```

</Instruction>

We hope you got a glimpse of the power that Prisma's generated mutations and queries offer. You used just three operations, but there are many more. Checkout the many queries and mutations that your Prisma service offers by running GraphQL Playground with `prisma playground`.

That's it, now you have a working mini blog application backed with Prisma!
