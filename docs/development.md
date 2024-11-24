[](){#development-reference}
# Development

Notes for developers. If you want to get involved, please do!
We welcome all kinds of contributions, for example:

- docs fixes/clarifications
- bug reports
- bug fixes
- feature requests
- pull requests
- tutorials

## Workflows

We don't mind whether you use a branching or forking workflow.
However, please only push to your own branches,
pushing to other people's branches is often a recipe for disaster,
is never required in our experience
so is best avoided.

Try and keep your merge requests as small as possible
(focus on one thing if you can).
This makes life much easier for reviewers
which allows contributions to be accepted at a faster rate.

## Language

We use British English for our development.
We do this for consistency with the broader work context of our lead developers.

## Versioning

This package follows the version format
described in [PEP440](https://peps.python.org/pep-0440/)
and [Semantic Versioning](https://semver.org/)
to describe how the version should change
depending on the updates to the code base.

Our changelog entries and compiled [changelog](./changelog.md)
allow us to identify where key changes were made.

## Changelog

We use [towncrier](https://towncrier.readthedocs.io/en/stable/)
to manage our changelog which involves writing a news fragment
for each Merge Request that will be added to the [changelog](./changelog.md) on the next release.
See the [changelog](https://github.com/CMIP-REF/cmip-ref/tree/main/changelog) directory
for more information about the format of the changelog entries.

## Dependency management

We manage our dependencies using [uv](https://docs.astral.sh/uv/).
This allows the ability to author multiple packages in a single repository,
and provides a consistent way to manage dependencies across all of our packages.
This mono-repo approach might change once the packages become more mature,
but since we are in the early stages of development,
there will be a lot of refactoring of the interfaces to find the best approach.

[](){releasing-reference}
## Releasing

Releasing is semi-automated via a CI job.
The CI job requires the type of version bump
that will be performed to be manually specified.
The supported bump types are:

* `major`
* `minor`
* `patch`

We don't yet support pre-release versions,
but this is something that we will consider in the future.

### Standard process

The steps required are the following:

1. Bump the version: manually trigger the "bump" workflow from the main branch
   (see here: [bump workflow](https://github.com/CMIP-REF/cmip-ref/actions/workflows/bump.yaml)).
   A valid "bump_rule" will need to be specified.
   This will then trigger a draft release.

1. Edit the draft release which has been created
   (see here:
   [project releases](https://github.com/CMIP-REF/cmip-ref/releases)).
   Once you are happy with the release (removed placeholders, added key
   announcements etc.) then hit 'Publish release'. This triggers the `release` workflow to
   PyPI (which you can then add to the release if you want).


1. That's it, release done, make noise on social media of choice, do whatever
   else

1. Enjoy the newly available version

## Read the Docs

Our documentation is hosted by
[Read the Docs (RtD)](https://www.readthedocs.org/),
a service for which we are very grateful.
The RtD configuration can be found in the `.readthedocs.yaml` file
in the root of this repository.
The docs are automatically deployed at
[cmip-ref.readthedocs.io](https://cmip-ref.readthedocs.io/en/latest/).
