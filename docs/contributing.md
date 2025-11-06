# Contributions 

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Installing Development Dependencies

```bash
pip install -e '.[dev]'
```

## Running Unit Tests

```bash
pip install tox tox-conda
tox -e py310
```

## Releasing

To release docs, run `make release-docs` in the root directory of the repository.
Make sure you have write permissions on the `gh-pages` branch.

We're currently working on releasing the package to an (internal or public)
package index.
