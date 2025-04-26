## Terminal Commands
- when using the terminal or executing a command on the command line, the conda environment for this project should be activated. Refer to the environemnt.yml file for the environment definition.
- if you have a terminal already open, make sure the right conda environemnt is set. Refer to the environemnt.yml file for the environment definition.

## testing
- when running all tests, start with unit tests before integration tests. All unit tests should pass before running integration tests.
- testing should display the pass/fail status of each test.
- Using PEP 420. 
- after create new tests run pip install -e .

## python
-PEP 420 is being used.

## test imports
-When writing or generating tests, do not use the 'src.' prefix in imports. Use imports relative to the package root (e.g., 'from collection.manager import ...'), as required by src-layout and PEP 420. This avoids ModuleNotFoundError issues.