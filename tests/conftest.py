# Ensure the src directory is in sys.path for test discovery and imports
import sys
import os
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import pytest
from dotenv import load_dotenv
import shutil

@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    """
    Automatically load .env.test for all tests in this session.
    """
    env_path = os.path.join(os.path.dirname(__file__), '../../.env.test')
    load_dotenv(dotenv_path=env_path, override=True)

@pytest.fixture
def storage_backend(request):
    """
    Fixture to dynamically set the STORAGE_BACKEND environment variable.

    This fixture can be used to switch between 'local' and 'azure' storage backends
    for integration tests without modifying the .env.test file.

    Usage:
        @pytest.mark.parametrize("storage_backend", ["local", "azure"], indirect=True)
        def test_something(storage_backend):
            # Test with the specified storage backend
            ...

    Returns:
        str: The current storage backend value
    """
    # Store the original value to restore it after the test
    original_value = os.environ.get('STORAGE_BACKEND')

    # Get the backend type from parametrize if available
    backend_type = getattr(request, 'param', 'local')

    # Set the environment variable
    os.environ['STORAGE_BACKEND'] = backend_type

    # Yield the current value
    yield backend_type

    # Restore the original value after the test
    if original_value is not None:
        os.environ['STORAGE_BACKEND'] = original_value
    else:
        os.environ.pop('STORAGE_BACKEND', None)

@pytest.fixture
def azure_backend():
    """
    Fixture to set the STORAGE_BACKEND environment variable to 'azure'.

    This fixture is a convenience wrapper around the storage_backend fixture
    that always sets the backend to 'azure'.

    Usage:
        def test_something_with_azure(azure_backend):
            # Test with Azure storage backend
            ...
    """
    # Store the original value to restore it after the test
    original_value = os.environ.get('STORAGE_BACKEND')

    # Set the environment variable to 'azure'
    os.environ['STORAGE_BACKEND'] = 'azure'

    # Yield the current value
    yield 'azure'

    # Restore the original value after the test
    if original_value is not None:
        os.environ['STORAGE_BACKEND'] = original_value
    else:
        os.environ.pop('STORAGE_BACKEND', None)

@pytest.fixture
def local_backend():
    """
    Fixture to set the STORAGE_BACKEND environment variable to 'local'.

    This fixture is a convenience wrapper around the storage_backend fixture
    that always sets the backend to 'local'.

    Usage:
        def test_something_with_local(local_backend):
            # Test with local storage backend
            ...
    """
    # Store the original value to restore it after the test
    original_value = os.environ.get('STORAGE_BACKEND')

    # Set the environment variable to 'local'
    os.environ['STORAGE_BACKEND'] = 'local'

    # Yield the current value
    yield 'local'

    # Restore the original value after the test
    if original_value is not None:
        os.environ['STORAGE_BACKEND'] = original_value
    else:
        os.environ.pop('STORAGE_BACKEND', None)

@pytest.fixture(scope="session", autouse=True)
def cleanup_test_system():
    """
    Cleanup the test_system directory after all tests complete.
    """
    yield
    test_system_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test_system'))
    if os.path.exists(test_system_path):
        shutil.rmtree(test_system_path, ignore_errors=True)
