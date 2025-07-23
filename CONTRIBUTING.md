# Contributing to 3T

First off, thank you for considering contributing to 3T! It's people like you that make open source such a great community.

## Where do I go from here?

If you've noticed a bug or have a feature request, [make one](https://github.com/tttrisagion/3T/issues/new)! It's generally best if you get confirmation of your bug or approval for your feature request this way before starting to code.

### Fork & create a branch

If this is something you think you can fix, then [fork 3T](https://github.com/tttrisagion/3T/fork) and create a branch with a descriptive name.

A good branch name would be (where issue #33 is the ticket you're working on):

```sh
git checkout -b 33-add-new-exchange-support
```

## Getting started

The `main` branch at `https://github.com/tttrisagion/3T` is the most stable and up-to-date version of the project. It is the branch that all pull requests should be made against.

To get started with the project, run `docker-compose up -d` to build and start the required services.

## Pull Requests

When you're done with your changes, create a pull request, also known as a PR.
- Don't forget to [link PR to issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) if you are solving one.
- Enable the checkbox to [allow maintainer edits](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork) so the branch can be updated for a merge.
Once you submit your PR, a maintainer will review your proposal. We may ask questions or request for additional information.
- We may ask for changes to be made before a PR can be merged, either using [suggested changes](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/incorporating-feedback-in-your-pull-request) or pull request comments. You can apply suggested changes directly through the UI. You can make any other changes in your fork, then commit them to your branch.
- As you update your PR and apply changes, a maintainer will continue to review and merge your pull request.
