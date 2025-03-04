# from sys import platform
import os
from dotenv import load_dotenv
import requests

from webviz_4d._providers.wellbore_provider._msal import get_token


def extract_omnia_session(omnia_path, api_name):
    load_dotenv(omnia_path)

    resource_key = api_name + "_RESOURCE"
    resource = os.environ.get(resource_key, None)

    subscription_key = api_name + "_SUBSCRIPTION_KEY"
    subscription = os.environ.get(subscription_key)

    scope = resource + "/.default"
    session = requests.session()

    token = get_token(scope)

    if token is None:
        print("ERROR: access token not acquired")
        session = None

    else:
        print("Create a new SMDA session \n")
        session.headers.update(
            {
                "Authorization": "Bearer " + token,
                "Ocp-Apim-Subscription-Key": subscription,
            }
        )

    return session
