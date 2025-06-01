from typing import Dict, Any
import uuid
from storage.path_strategy import OHLCVPathStrategy
from exchange_source.models import Metadata


class OHLCVTestPathStrategy(OHLCVPathStrategy):
    """Test version of OHLCVPathStrategy that prepends a test prefix for isolation."""
    
    def __init__(self):
        """
        Initialize with a test prefix using convention test-{6-char-uuid}.
        """
        test_prefix = f"test-{uuid.uuid4().hex[:6]}"
        self.test_prefix = test_prefix + '/'
    
    def generate_base_path(self, context: Dict[str, Any]) -> str:
        """Generate the base path with test prefix prepended."""
        base_path = super().generate_base_path(context)
        return f"{self.test_prefix}{base_path}"
    
    def generate_path_prefix(self, context: Dict[str, Any]) -> str:
        """Generate a path prefix with test prefix prepended."""
        prefix = super().generate_path_prefix(context)
        return f"{self.test_prefix}{prefix}"
    
    def get_metadata(self, path: str) -> Metadata:
        """Extract metadata by removing test prefix first."""
        # Remove test prefix if present
        if self.test_prefix and path.startswith(self.test_prefix):
            path = path[len(self.test_prefix):]
        return super().get_metadata(path)
