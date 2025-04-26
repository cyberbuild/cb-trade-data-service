# TargetManager implementation for managing the target list using Delta Lake
# This is a stub; full implementation will follow after directory creation.

import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
from deltalake import DeltaTable, write_deltalake

class TargetManager:
    DEFAULT_SCHEMA = {
        "target_id": "string",
        "coin": "string",
        "exchange": "string",
        "exchange_id": "string",
        "interval": "string",
        "enabled": "boolean",
        "last_updated": "datetime64[ns]",
    }

    def __init__(self, config: Dict[str, Any]):
        self.storage_type = config.get("TARGET_STORAGE_TYPE", "local")
        self.table_path = config.get("TARGET_TABLE_PATH", "./targets_delta")
        self.azure_options = config.get("AZURE_STORAGE_OPTIONS", None)
        # Patch for Azure: add account_name if using Azure
        if self.azure_options and "connection_string" in self.azure_options:
            conn_str = self.azure_options["connection_string"]
            # Extract account_name and account_key from connection string
            for part in conn_str.split(";"):
                if part.lower().startswith("accountname="):
                    self.azure_options["account_name"] = part.split("=", 1)[1]
                if part.lower().startswith("accountkey="):
                    self.azure_options["account_key"] = part.split("=", 1)[1]
            # Remove connection_string, keep only account_name, account_key, container
            self.azure_options = {
                k: v for k, v in self.azure_options.items()
                if k in ("account_name", "account_key", "container")
            }
        self._init_table()

    def _init_table(self):
        # Try to load the DeltaTable, create if not exists
        try:
            self.table = DeltaTable(self.table_path, storage_options=self.azure_options)
        except Exception:
            # Table does not exist, create it
            df = pd.DataFrame(columns=self.DEFAULT_SCHEMA.keys())
            df = df.astype(self.DEFAULT_SCHEMA)
            write_deltalake(self.table_path, df, mode="overwrite", storage_options=self.azure_options)
            self.table = DeltaTable(self.table_path, storage_options=self.azure_options)

    def add_target(self, target: Dict[str, Any]):
        target["last_updated"] = datetime.utcnow()
        df = pd.DataFrame([target])
        write_deltalake(self.table_path, df, mode="append", storage_options=self.azure_options)
        self.table = DeltaTable(self.table_path, storage_options=self.azure_options)

    def update_target(self, target_id: str, updates: Dict[str, Any]):
        updates["last_updated"] = datetime.utcnow()
        # Always use DataFrame-based update for both local and Azure
        df = self.table.to_pandas()
        mask = df["target_id"] == target_id
        for k, v in updates.items():
            df.loc[mask, k] = v
        write_deltalake(self.table_path, df, mode="overwrite", storage_options=self.azure_options if self.storage_type == "azure" else None)
        if self.storage_type == "azure":
            self.table = DeltaTable(self.table_path, storage_options=self.azure_options)
        else:
            self.table = DeltaTable(self.table_path)

    def delete_target(self, target_id: str):
        if self.storage_type == "local":
            # Read table to DataFrame
            df = self.table.to_pandas()
            df = df[df["target_id"] != target_id]
            
            # If DataFrame is empty, we need a special approach
            if df.empty:
                import shutil
                # For empty tables, recreate the directory structure
                table_path = self.table_path
                if os.path.exists(table_path):
                    shutil.rmtree(table_path)
                os.makedirs(table_path, exist_ok=True)
                
                # Initialize empty schema
                df = pd.DataFrame(columns=list(self.DEFAULT_SCHEMA.keys()))
                df = df.astype(self.DEFAULT_SCHEMA)
            
            # Write back the filtered DataFrame
            write_deltalake(self.table_path, df, mode="overwrite")
            self.table = DeltaTable(self.table_path)
        else:
            # Azure/remote: DataFrame-based delete workaround
            df = self.table.to_pandas()
            df = df[df["target_id"] != target_id]
            # If DataFrame is empty, re-create with empty schema
            if df.empty:
                import shutil
                table_path = self.table_path
                if os.path.exists(table_path):
                    shutil.rmtree(table_path)
                os.makedirs(table_path, exist_ok=True)
                df = pd.DataFrame(columns=list(self.DEFAULT_SCHEMA.keys()))
                df = df.astype(self.DEFAULT_SCHEMA)
            write_deltalake(self.table_path, df, mode="overwrite", storage_options=self.azure_options)
            self.table = DeltaTable(self.table_path, storage_options=self.azure_options)

    def get_target(self, target_id: str) -> Optional[Dict[str, Any]]:
        df = self.table.to_pandas()
        result = df[df["target_id"] == target_id]
        if not result.empty:
            return result.iloc[0].to_dict()
        return None

    def list_targets(self, enabled: Optional[bool] = None) -> List[Dict[str, Any]]:
        df = self.table.to_pandas()
        if enabled is not None:
            df = df[df["enabled"] == enabled]
        return df.to_dict(orient="records")
