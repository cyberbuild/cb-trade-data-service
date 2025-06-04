# filepath: c:\Project\cyberbuild\cb-trade\cb-trade-data-service\src\secrets\interfaces.py
import abc
from pydantic import SecretStr
from typing import Optional


class ISecretProvider(abc.ABC):
    """Interface for retrieving secrets from different sources."""

    @abc.abstractmethod
    def get_secret(self, secret_name: str) -> Optional[SecretStr]:
        """
        Retrieves a secret value by its name.
        Returns None if the secret is not found.
        """
        pass
