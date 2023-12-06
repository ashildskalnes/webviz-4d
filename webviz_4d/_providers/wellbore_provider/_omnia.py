from sys import platform
import os
from dotenv import load_dotenv
import requests

from webviz_4d._providers.wellbore_provider._msal import msal_get_token


def extract_omnia_session(omnia_path, api_name):
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

    home = os.path.expanduser("~")
    token_name = api_name + "_token_cache.bin"
    cache_filename = os.path.join(home, token_name)

    result = msal_get_token(CLIENT_ID, AUTHORITY, scope, cache_filename)
    session = requests.session()

    if "access_token" in result:
        session.headers.update(
            {
                "Authorization": "Bearer " + result["access_token"],
                "Ocp-Apim-Subscription-Key": subscription,
            }
        )
    else:
        print("Could not connect to", api_name, "Error:", result.get("error"))
        print(result.get("error_description"), "\n")

    return session
