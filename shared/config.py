import copy
import os
from pathlib import Path
from typing import Any, Dict, Optional, Type

import yaml


class Config:
    _instance: Optional["Config"] = None
    app_config: Dict[str, Any]
    secrets_config: Dict[str, Any]

    # Define all code-based defaults here. These values will be used if not overridden
    # in config.yml. Values from config.yml always take precedence.
    _DEFAULTS: Dict[str, Any] = {
        "reconciliation_engine": {
            # This value is the minimum trade threshold as a percentage of the relevant
            # balance (e.g., account or asset balance).
            # Default: 0.061402432 (approx 6.14%)
            "minimum_trade_threshold": 0.061402432,
        }
    }

    def __new__(cls: Type["Config"]) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.load_config()
        return cls._instance

    def _deep_update(
        self, target: Dict[str, Any], update_from: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Recursively updates a dictionary `target` with values from `update_from`.
        Values in `update_from` take precedence over values in `target`.
        """
        for k, v in update_from.items():
            if isinstance(v, dict) and k in target and isinstance(target[k], dict):
                # If both target and update_from have a dictionary for this key, recurse
                target[k] = self._deep_update(target[k], v)
            else:
                # Otherwise, update or add the value (file value takes precedence)
                target[k] = v
        return target

    def load_config(self) -> None:
        base_dir = Path(__file__).resolve().parent.parent

        # Start with a deep copy of defaults to ensure modifications don't alter _DEFAULTS
        self.app_config = copy.deepcopy(self._DEFAULTS)

        # Load main config from file and merge, allowing file values to take precedence.
        config_path = base_dir / "config.yml"
        if os.path.exists(config_path):
            with open(config_path) as f:
                file_config = yaml.safe_load(f)

            # This handles cases where config.yml is empty or not a dictionary.
            if isinstance(file_config, dict):
                self.app_config = self._deep_update(self.app_config, file_config)
            # If file_config is not a dictionary (e.g., None for empty file, or a scalar),
            # we ignore it and continue with the defaults. This prevents errors from malformed files.

        # Load secrets
        secrets_path = base_dir / "secrets.yml"
        if os.path.exists(secrets_path):
            with open(secrets_path) as f:
                # Ensure secrets_config is a dict even if the file is empty
                self.secrets_config = yaml.safe_load(f) or {}
        else:
            # In a containerized environment, you might want to handle this differently,
            # e.g., by reading from environment variables or a secrets management service.
            # For now, we'll raise an error if the file is missing.
            raise FileNotFoundError(
                f"secrets.yml not found at {secrets_path}. Please create it from secrets.yml.example."
            )

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a value from the application config using dot notation.
        e.g., 'database.host'
        """
        keys = key_path.split(".")
        value = self.app_config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def get_secret(self, key_path: str, default: Any = None) -> Any:
        """
        Get a value from the secrets config using dot notation.
        e.g., 'database.password'
        """
        keys = key_path.split(".")
        value = self.secrets_config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value


# Global config instance
config = Config()