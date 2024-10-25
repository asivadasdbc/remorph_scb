import base64
from unittest.mock import create_autospec

import pytest

from src.databricks.labs.remorph.reconcile.connectors.secrets import SecretsMixin
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import GetSecretResponse


class Test(SecretsMixin):
    def __init__(self, ws: WorkspaceClient, secret_scope: str):
        self._ws = ws
        self._secret_scope = secret_scope


def mock_secret(scope, key):
    secret_mock = {
        "scope": {
            'user_name': GetSecretResponse(
                key='user_name', value=base64.b64encode(bytes('my_user', 'utf-8')).decode('utf-8')
            ),
            'password': GetSecretResponse(
                key='password', value=base64.b64encode(bytes('my_password', 'utf-8')).decode('utf-8')
            ),
        }
    }

    return secret_mock.get(scope).get(key)


def test_get_secrets_happy():
    ws = create_autospec(WorkspaceClient)
    ws.secrets.get_secret.side_effect = mock_secret

    mock = Test(ws, "scope")

    assert mock._get_secret("user_name") == "my_user"
    assert mock._get_secret("password") == "my_password"


def test_get_secrets_not_found_exception():
    ws = create_autospec(WorkspaceClient)
    ws.secrets.get_secret.side_effect = NotFound("Test Exception")
    mock = Test(ws, "scope")

    with pytest.raises(NotFound, match="Secret does not exist with scope: scope and key: unknown : Test Exception"):
        mock._get_secret("unknown")
