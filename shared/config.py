import os
from pathlib import Path

import yaml


class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.load_config()
        return cls._instance

    def load_config(self):
        base_dir = Path(__file__).resolve().parent.parent

        # Load main config
        with open(base_dir / "config.yml") as f:
            self.app_config = yaml.safe_load(f)

        # Load secrets
        secrets_path = base_dir / "secrets.yml"
        if os.path.exists(secrets_path):
            with open(secrets_path) as f:
                self.secrets_config = yaml.safe_load(f)
        else:
            # In a containerized environment, you might want to handle this differently,
            # e.g., by reading from environment variables or a secrets management service.
            # For now, we'll raise an error if the file is missing.
            raise FileNotFoundError(
                f"secrets.yml not found at {secrets_path}. Please create it from secrets.yml.example."
            )

    def get(self, key_path, default=None):
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

    def get_secret(self, key_path, default=None):
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
