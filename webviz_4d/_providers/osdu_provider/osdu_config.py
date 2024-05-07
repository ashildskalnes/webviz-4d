import os
from dotenv import load_dotenv
from typing import Optional
from osdu_api.configuration.base_config_manager import BaseConfigManager
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel


class Config(BaseSettings, BaseConfigManager):
    INSTANCE: str = "https://npequinor.energy.azure.com"
    DATA_PARTITION_ID: str = "npequinor-dev"
    LEGAL_TAG: str = "npequinor-dev-equinor-private-default"
    TENANT_ID: str = "3aa4a235-b6e2-48d5-9195-7fcf05b459b0"
    ACCESS_TOKEN: str = f"{INSTANCE}/dummy"  # TODO: Only temp solution
    # Below is required naming for osdu-api library
    STORAGE_URL: str = f"{INSTANCE}/api/storage/v2"
    SEARCH_URL: str = f"{INSTANCE}/api/search/v2"
    LEGAL_URL: str = f"{INSTANCE}/api/legal/v1"
    FILE_DMS_URL: str = f"{INSTANCE}/api/file/v2"
    DATASET_URL: str = f"{INSTANCE}/api/dataset/v1"
    ENTITLEMENTS_URL: str = f"{INSTANCE}/api/entitlements/v1"
    SCHEMA_URL: str = f"{INSTANCE}/api/schema-service/v1"
    INGESTION_WORKFLOW_URL: str = f"{INSTANCE}/api/workflow/v1"

    if ACCESS_TOKEN == f"{INSTANCE}/dummy":
        load_dotenv(
            dotenv_path=os.path.join(os.path.expanduser("~"), ".env"), override=True
        )
        ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

    def get(self, section: str, option: str, default: Optional[str] = None) -> str:
        return self.model_dump().get(option.upper(), default)

    def getint(self, section: str, option: str, default: Optional[int] = None) -> int:
        return self.model_dump().get(option.upper(), default)

    def getfloat(
        self, section: str, option: str, default: Optional[float] = None
    ) -> int:
        return self.model_dump().get(option.upper(), default)

    def getbool(
        self, section: str, option: str, default: Optional[bool] = None
    ) -> bool:
        return self.model_dump().get(option.upper(), default)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
