import json
import msal
import sys
from msal_extensions import *
from msal_extensions import persistence


def msal_cache_accounts(clientID, authority, cache_filename):
    # Based on https://gist.github.com/darrenjrobinson/553ea10e304246ebfa1eac6dde0cf63b

    persistence = build_encrypted_persistence(cache_filename)
    cache = PersistedTokenCache(persistence)

    app = msal.PublicClientApplication(
        client_id=clientID, authority=authority, token_cache=cache
    )
    accounts = app.get_accounts()
    return accounts


def msal_delegated_refresh(clientID, scope, authority, account, cache_filename):
    persistence = build_encrypted_persistence(cache_filename)
    cache = PersistedTokenCache(persistence)

    app = msal.PublicClientApplication(
        client_id=clientID, authority=authority, token_cache=cache
    )
    result = app.acquire_token_silent_with_error(
        scopes=[scope], account=account, force_refresh=True
    )
    return result


def msal_delegated_device_flow(clientID, scope, authority, cache_filename):
    print("Initiate Device Code Flow to get an AAD Access Token.")
    print(
        "Open a browser window and paste in the URL below and then enter the Code. CTRL+C to cancel."
    )

    persistence = build_encrypted_persistence(cache_filename)
    cache = PersistedTokenCache(persistence)

    app = msal.PublicClientApplication(
        client_id=clientID, authority=authority, token_cache=cache
    )
    flow = app.initiate_device_flow(scopes=[scope])

    if "user_code" not in flow:
        raise ValueError(
            "Fail to create device flow. Err: %s" % json.dumps(flow, indent=4)
        )

    print(flow["message"])
    sys.stdout.flush()

    result = app.acquire_token_by_device_flow(flow)
    return result


def msal_get_token(CLIENT_ID, AUTHORITY, SCOPE, cache_filename):
    accounts = msal_cache_accounts(CLIENT_ID, AUTHORITY, cache_filename)

    result = None
    if accounts:
        for account in accounts:
            myAccount = account
            # print("Found account in MSAL Cache: " + account["username"])
            # print("Obtaining a new Access Token using the Refresh Token")
            result = msal_delegated_refresh(
                CLIENT_ID, SCOPE, AUTHORITY, myAccount, cache_filename
            )
    if result is None or result.get("error"):
        print(f"Cashed token not found, activating a new token")
        result = msal_delegated_device_flow(CLIENT_ID, SCOPE, AUTHORITY, cache_filename)
    if result.get("error"):
        print(f"Unable to authorize. {result['error_description']}")
    return result
