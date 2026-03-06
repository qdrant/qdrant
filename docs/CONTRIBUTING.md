# Contributing to Qdrant
We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features

## We Develop with GitHub
We use github to host code, to track issues and feature requests, as well as accept pull requests.

## We Use [GitHub Flow](https://guides.github.com/introduction/flow/index.html), So All Code Changes Happen Through Pull Requests
Pull requests are the best way to propose changes to the codebase (we use [GitHub Flow](https://docs.github.com/en/get-started/quickstart/github-flow)). We actively welcome your pull requests:

1. Fork the repo and create your branch from `dev`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation and API Schema definitions (see [development docs](https://github.com/qdrant/qdrant/blob/master/docs/DEVELOPMENT.md#api-changes))
4. Ensure the test suite passes.
5. Make sure your code lints (with cargo).
6. Issue that pull request!

## Any contributions you make will be under the Apache License 2.0
In short, when you submit code changes, your submissions are understood to be under the same [Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/) that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using GitHub's [issues](https://github.com/qdrant/qdrant/issues)
We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/qdrant/qdrant/issues/new/choose); it's that easy!

## Write bug reports with detail, background, and sample code

**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Give sample code if you can.
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

## Use a Consistent Coding Style

If you are modifying Rust code, make sure it has no warnings from Cargo and follow [Rust code style](https://doc.rust-lang.org/1.0.0/style/).
The project uses [rustfmt](https://github.com/rust-lang/rustfmt) formatter. Please ensure to run it using the
```cargo +nightly fmt --all``` command. The project also use [clippy](https://github.com/rust-lang/rust-clippy) lint collection,
so please ensure running ``cargo clippy --workspace --all-features`` before submitting the PR.

## Contributing with AI

At Qdrant, we recognize the usefulness of AI tools in software development.
At the same time, we also observe the growing problem of a high volume of contributions, which require careful reviews.

To compensate for the lack of human attention budget, while keeping the quality and efficiency of contributions,
we introduce the following principles for contributions with AI:

#### Do not communicate with real people through AI

When preparing a PR description or answering to comments, please avoid using AI tools to generate the content.
We want to see PR documentation as a source of truth, so we can validate if the changes are actually following the intent.
Avoid including AI-generated diagrams or walkthroughs of the code; if somebody would like to read them, they can always generate them themselves.

#### If you are using AI tools to generate PR, you are still responsible for the results

We expect you to understand the changes you propose and be able to answer questions about it.
"I asked claude, and it generated this" is not an acceptable answer, and we might discard the PR if we find it hard to understand the changes.

#### Disclose if some parts of the PR are generated with AI tools

A good practice is to explicitly specify which commits are made using AI tools, and which commits are manual changes.

For example, a good PR description with AI-generated commits:

```
- [AI] feat: create a new fusion method for hybrid search
- [manual] fix: improve mutex handling in the new fusion method
```

#### Disclose initial prompt you used to generate the contribution

Code generation tools have the ability to perform modifications across the whole Qdrant codebase, so
it becomes increasingly important to know what was the original intent communicated to the AI tool.

Sometimes, the problem may exist in the prompt itself, and even if the generated code is flawless, 
it may cause issues which are hard to spot in a review.

Example of a bad prompt:

```
Find bugs in the module and fix them
```

Try to keep your prompt as specific as possible, leaving no room for misinterpretation.
If you are making incremental changes, please try to submit them as separate commits,
so we can easily understand the intent of each change.


## License
By contributing, you agree that your contributions will be licensed under its Apache License 2.0.

