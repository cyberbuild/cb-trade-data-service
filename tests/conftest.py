import os
import pytest
from dotenv import load_dotenv

@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    """
    Automatically load .env.test for all tests in this session.
    """
    env_path = os.path.join(os.path.dirname(__file__), '../../.env.test')
    load_dotenv(dotenv_path=env_path, override=True)
