## Rules
- Do not put files edits in the chat window, update the 
- Code is self describing. 
- Do not litter the code with useless commenting. The code is not your notebook, and not for teaching coding.
- full implementations. No "concepts", No "Demo", No "Sample", No "Stub"
- Do not create comments in the code describing the change that was made. The code is self describing.

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
-DO NOT Create __init__.py files.
- don't leave whitespace on blank lines W293
- don't add trailing whitespace to lines W291

## test imports
-When writing or generating tests, do not use the 'src.' prefix in imports. Use imports relative to the package root (e.g., 'from collection.manager import ...'), as required by src-layout and PEP 420. This avoids ModuleNotFoundError issues.
-Make sure to use test objects from a test helper class of sort. This can be used for injection. Things are built against interfaces. So in the beginning it may be a mock object, later it may be an implementation. This is done to have consistent behaviour across tests.
-Always run pytest with -n auto > unless you're only runing 1 or 2 tests. You could specify how many workers if 24 or less test.