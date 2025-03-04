import os
from dotenv import load_dotenv
from msal import PublicClientApplication
import requests

home = os.path.expanduser("~")
env_path = os.path.expanduser(os.path.join(home, ".omniaapi"))
load_dotenv(dotenv_path=env_path)


# Azure AD config
TENANT = os.environ.get("TENANT")
CLIENT_ID = os.environ.get("WEBVIZ_4D_ID")
AUTHORITY = "https://login.microsoftonline.com/" + TENANT

_app = PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
_session = requests.Session()


def get_token(SCOPE):
    account = None
    accounts = _app.get_accounts()
    if accounts:
        account = accounts[0]  # assuming single user
        print(f"Logged in as {account['username']}")
    result = _app.acquire_token_silent([SCOPE], account=account)
    if not result:
        # flow = _app.initiate_device_flow([SCOPE])
        # print(flow['message'])
        result = _app.acquire_token_interactive([SCOPE])
    if "access_token" in result:
        return result["access_token"]
    else:
        print(f"Error - {result.get('error')} - {result.get('error_description')}")
        return None
