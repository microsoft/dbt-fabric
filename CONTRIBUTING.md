# Development of the adapter

We recommend creating a virtual environment to develop the adapter. At the time of writing, [`uv`](https://docs.astral.sh/uv/) is a very popular tool to work with Python packages and environments. Installation should be pretty straightforward ([docs](https://docs.astral.sh/uv/getting-started/installation/)).

Throughout the rest of this guide, we'll assume you're using `uv`. `uv` is a drop-in replacement for `pip` with greater performance and additional features. You can of course use any other tool you prefer.

Uv is super simple to use and only requires you to run a single command to do the following:

1. Create a virtual environment
1. Install all dependencies
1. Install the adapter in an editable mode
1. Install the development dependencies

```shell
uv sync
```

To run anything inside the virtual environment, use `uv run ...`. Otherwise, you can [activate the virtual environment](https://docs.astral.sh/uv/pip/environments/#using-a-virtual-environment) before running any commands.

## Testing

The functional tests require a Fabric Data Warehouse. Tell our tests how they should connect to your data warehouse by creating a file called `test.env` in the root of the project.
You can use the provided `test.env.sample` as a base.

```shell
cp test.env.sample test.env
```

You can use the following commands to run the unit and the functional tests respectively:

```shell
uv run pytest tests/unit
uv run pytest tests/functional
```

## CI/CD

We use Docker images that have all the things we need to test the adapter in the CI/CD workflows.
The Dockerfile is located in the *.github* directory and pushed to GitHub Packages to this repo.
There is one tag per supported Python version.

All CI/CD pipelines are using GitHub Actions. The following pipelines are available:

* `publish-docker`: publishes the image we use in all other pipelines.
* `unit-tests`: runs the unit tests for each supported Python version.
* `integration-tests`: runs the integration tests.
* `release-version`: publishes the adapter to PyPI.

## Releasing a new version

Make sure the version number is bumped in `__version__.py`. Then, create a git tag named `v<version>` and push it to GitHub.
A GitHub Actions workflow will be triggered to build the package and push it to PyPI. 
