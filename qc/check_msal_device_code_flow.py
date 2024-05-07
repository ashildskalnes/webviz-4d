from sys import platform
import os
from dotenv import load_dotenv
import json
import requests
from msal import ClientApplication
from webviz_4d._providers.wellbore_provider._provider_impl_file import (
    ProviderImplFile,
)

from webviz_4d._datainput.well import (
    load_all_wells,
    load_smda_metadata,
)

def main():
    # Initiate
    omnia_env = ".omniaapi"
    api_name = "SMDA"
    home = os.path.expanduser("~")
    omnia_path = os.path.expanduser(os.path.join(home, omnia_env))
    load_dotenv(omnia_path)

    if platform == "linux" or platform == "linux2":
        os.environ[
            "REQUESTS_CA_BUNDLE"
        ] = "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"

    TENANT = os.environ.get("TENANT")
    AUTHORITY = "https://login.microsoftonline.com/" + TENANT
    CLIENT_ID = os.environ.get("WEBVIZ_4D_ID")

    resource_key = api_name + "_RESOURCE"
    resource = os.environ.get(resource_key, None)
    scope = resource + "/user_impersonation"

    subscription_key = api_name + "_SUBSCRIPTION_KEY"
    subscription = os.environ.get(subscription_key)

    client_secret = os.environ.get("WEBVIZ_4D_SECRET")

    omnia_env = ".omniaapi"
    home = os.path.expanduser("~")
    env_path = os.path.expanduser(os.path.join(home, omnia_env))
    smda_provider = ProviderImplFile(env_path, "SMDA")

    field_name = "JOHAN SVERDRUP"
    print("Loading drilled well data from SMDA ...")
    drilled_wells_info = load_smda_metadata(
        smda_provider, field_name
    )

    print(drilled_wells_info)
  

if __name__ == "__main__":
    main()
    

