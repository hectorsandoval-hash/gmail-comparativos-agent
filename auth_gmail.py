"""
Modulo de autenticacion OAuth2 con Gmail, Drive y Sheets API.
Maneja el flujo de autorizacion y almacenamiento de tokens.
"""
import os
import httplib2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google_auth_httplib2 import AuthorizedHttp

from config import CREDENTIALS_FILE, TOKEN_FILE, SCOPES

# Timeout HTTP para llamadas a Google APIs (segundos)
HTTP_TIMEOUT = 30


_creds = None


def _obtener_credenciales():
    """Obtiene credenciales OAuth2, reutilizando si ya existen."""
    global _creds

    if _creds and _creds.valid:
        return _creds

    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("[AUTH] Refrescando token expirado...")
            creds.refresh(Request())
        else:
            # En GitHub Actions no hay navegador, solo funciona con token existente
            if os.environ.get("GITHUB_ACTIONS"):
                raise RuntimeError(
                    "[AUTH] Token expirado o invalido en GitHub Actions. "
                    "Regenera token.json localmente y actualiza el Secret GOOGLE_TOKEN."
                )

            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"Archivo de credenciales no encontrado: {CREDENTIALS_FILE}"
                )

            print("[AUTH] Iniciando flujo de autorizacion OAuth2...")
            print("[AUTH] Se abrira el navegador para autorizar acceso a Gmail + Drive + Sheets.")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
        print("[AUTH] Token guardado exitosamente.")

    _creds = creds
    return creds


def _build_service(api, version):
    """Construye un servicio de Google API con timeout HTTP."""
    creds = _obtener_credenciales()
    http = httplib2.Http(timeout=HTTP_TIMEOUT)
    authorized_http = AuthorizedHttp(creds, http=http)
    return build(api, version, http=authorized_http)


def autenticar_gmail():
    """Retorna el servicio de Gmail API."""
    service = _build_service("gmail", "v1")
    print("[AUTH] Conectado a Gmail API correctamente.")
    return service


def autenticar_drive():
    """Retorna el servicio de Google Drive API."""
    service = _build_service("drive", "v3")
    print("[AUTH] Conectado a Drive API correctamente.")
    return service


def autenticar_sheets():
    """Retorna el servicio de Google Sheets API."""
    service = _build_service("sheets", "v4")
    print("[AUTH] Conectado a Sheets API correctamente.")
    return service


def obtener_perfil(service):
    """Obtiene el perfil del usuario autenticado."""
    profile = service.users().getProfile(userId="me").execute()
    return profile.get("emailAddress", "desconocido")
