# Contributing

## Getting started

Before you begin:

- The codebase supports python >= 3.8
- The requirements.txt are all the dependencies needed to run the unit tests for all features
- **extra** requirements are listed in setup.py

### Ready to make a change? Fork the repo.

Fork using GitHub Desktop:

- [Getting started with GitHub Desktop](https://docs.github.com/en/desktop/installing-and-configuring-github-desktop/getting-started-with-github-desktop) will guide you through setting up Desktop.
- Once Desktop is set up, you can use it to [fork the repo](https://docs.github.com/en/desktop/contributing-and-collaborating-using-github-desktop/cloning-and-forking-repositories-from-github-desktop)!

Fork using the command line:

- [Fork the repo](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo#fork-an-example-repository) so that you can make your changes without affecting the original project until you're ready to merge them.

### Make your changes

The code follows the [PEP-8](https://peps.python.org/pep-0008/) style guide, and generally uses the default intellij/pycharm formatting rules.

The code uses python3.8 as the lowest common denominator, with some minor compatibility import tricks for future versions (e.g. 3.9, 3.10)

Make sure to run all the unit tests after modifying the code.

Additional automated tests which should be run (are not run automatically on commit to github due to constraints):
- Integration and languate interoperability tests at **examples/test_examples.py** (Requires java and building the 
  sample java code under **examples/java**)
- Tutorial guide tests under **examples/tutorial/test_tutorials.py** which are the end results of the
  [guide](https://rsocket.io/guides/rsocket-py/tutorial) at [rsocket.io](http://rsocket.io).

#### Testing

Almost all tests which are not for very low level code run using all the supported 
transports. You may temporeraly disable some of the transports to speed up testing using the **tested_transports** list in **tests/conftest.py**

### Open a pull request

Once you are done pushing changes to your fork, then you'll need to open a pull request (PR) to propose them for review.
Inside the PR the unit tests for all supported python version will be run. Make sure your changes pass on all versions.

Once you've validated your changes feel free to let us know in a comment or edit your initial comment of the PR.
