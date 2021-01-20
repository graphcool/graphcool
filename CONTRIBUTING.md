# Contribution Guidelines

Please read this guide if you're interested in contributing to Prisma.

**We welcome any form of contribution, especially from new members of our community** 💚

## Discussions

**Our community is a safe and friendly environment, where we support and treat each other with respect**.

We invite you to actively participate in discussions on Github, [the Forum](https://www.prisma.io/forum/) and [Slack](https://slack.prisma.io/).

You'll see many discussions about usage or design questions, but any topic is welcome.
They are a great foundation to find potential issues, feature requests or documentation improvements.

## Issues

To report a bug, you can use [this template](https://github.com/prisma/prisma/issues/new?template=bug_report.md).

New bug reports require to be triaged. Oftentimes this includes creating a minimal reproduction, or verifying that the problem still occurs on the latest stable version or the unstable channel (see below).

When you're starting to look into fixing a bug, create a WIP PR that you mention in the original issue. This way, we ensure that everyone interested can share their thoughts, and duplicate work is prevented.

Adding tests is a great way to help preventing future bugs.

## Documentation

You can either improve existing content or add new resources.

If you miss a particular information in [the reference documentation](https://www.prisma.io/docs), feel free to either create an issue or PR. We also welcome [tutorials](https://www.prisma.io/docs/tutorials) for related topics that you're interested in. If you come across an interesting pattern, feel free to add a new example [to our example list](https://github.com/prisma/prisma/tree/master/examples)!

## Release Channels

Releases are separated into three _channels_: **alpha**, **beta** and **stable**. You can learn more about these three channels and Prisma's release process [here](https://www.prisma.io/blog/improving-prismas-release-process-yaey8deiwaex/).

## Features

To request a new feature, you can use [this template](https://github.com/prisma/prisma/issues/new?template=feature_request.md).

To contribute features or API changes, it's best to start a discussion in an issue first, ideally with a proposal. This allows everyone to participate and ensures that no potential implementation work is in vain.

## Submitting Changes

After getting some feedback, push to your fork and submit a pull request. We
may suggest some changes or improvements or alternatives, but for small changes
your pull request should be accepted quickly.

Here are further contribution instructions:

* [`cli`](./cli/CONTRIBUTING.md)
* [`examples`](https://github.com/prisma/prisma-examples/blob/master/CONTRIBUTING.md)
* [`server`](./server/CONTRIBUTING.md)

For `cli` and `server`, **PRs need to be made against the `alpha` branch** (not `master`).

## Logistics

Below are a series of steps to help you from a more "practical" perspective. If you need additional help concerning Git, Github has some great guides that you may want to check out! Check them out [here](https://guides.github.com/)

**Fork the repo and then clone your fork**

SSH:
```sh
git clone git@github.com:YOUR_USERNAME/prisma.git
```

HTTPS:
```sh
git clone https://github.com/YOUR_USERNAME/prisma.git
```

**Add the remote upstream**

```sh
git remote add upstream git://github.com/prisma/prisma.git
```

**Fetch changes from upstream**

```sh
git fetch upstream
```

**Pull changes locally**

```sh
git pull upstream master
```

**Push changes up**

```sh
git push
```


# Labels

Read more about our labels [here](https://github.com/prisma/prisma/blob/master/.github/LABELS.md)
