"""
Configuracion central del agente de comparativos.
Datos sensibles se leen de variables de entorno (GitHub Secrets)
o del archivo .env local (no se sube al repo).
"""
import os
import json

# Ruta base del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================
# Cargar .env local si existe (para desarrollo local)
# En GitHub Actions, las variables vienen de Secrets
# ============================================================
_env_file = os.path.join(BASE_DIR, ".env")
if os.path.exists(_env_file):
    with open(_env_file, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if "=" in _line and not _line.startswith("#"):
                _key, _val = _line.split("=", 1)
                _key = _key.strip()
                if _key not in os.environ:
                    os.environ[_key] = _val.strip()

# ============================================================
# MODO PRUEBA: Solo envia reportes al usuario (no a otros)
# Cambiar a False cuando se quiera enviar a todos los destinatarios
# ============================================================
MODO_PRUEBA = False

# Archivos de credenciales OAuth2
CREDENTIALS_FILE = os.path.join(BASE_DIR, "credentials.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.json")

# Scopes necesarios para Gmail + Drive + Sheets API
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# Directorio temporal para descargar archivos
TEMP_DIR = os.path.join(BASE_DIR, "temp_files")

# ============================================================
# DATOS SENSIBLES (desde variables de entorno / GitHub Secrets)
# ============================================================

# Nombre del usuario (para columnas del reporte y seguimiento)
USUARIO_NOMBRE = os.environ.get("USUARIO_NOMBRE", "Usuario")

# Personas clave que deben estar en copia
_personas_json = os.environ.get("PERSONAS_CLAVE_JSON", "")
PERSONAS_CLAVE = json.loads(_personas_json) if _personas_json else {}

# Palabras que indican que un mensaje NO requiere nueva respuesta
# (son confirmaciones, traslados, generacion de OC, creacion de ordenes, etc.)
PALABRAS_NO_REQUIERE_RESPUESTA = [
    "de acuerdo", "ok", "conforme", "aprobado", "aprobada",
    "proceder", "procedemos", "se procede",
    "generar oc", "orden de compra", "se adjunta oc", "envio de oc",
    "se genero", "se generó", "se emitió", "se emitio",
    "adjunto oc", "oc aprobada", "oc generada",
    "trasladar", "traslado", "se traslada",
    # Creacion de ordenes (NO es observacion ni actualizacion del comparativo)
    "nueva orden", "se creó la orden", "se creo la orden",
    "se creó una orden", "se creo una orden", "se creó una nueva orden",
    "se creo una nueva orden", "orden creada", "orden de compra creada",
    "se genero la orden", "se generó la orden",
]

# Palabras clave para buscar comparativos y cotizaciones
SEARCH_KEYWORDS = [
    "comparativo", "comparativos", "cuadro comparativo",
    "cotización", "cotizacion", "cotizaciones",
    "\"c.c.\"", "\"cc.\"",
]

# Rango de busqueda en dias (solo correos de los ultimos N dias)
DIAS_BUSQUEDA = 7

# Query de busqueda en Gmail (combinacion OR + filtro de fecha)
GMAIL_SEARCH_QUERY = f"({' OR '.join(SEARCH_KEYWORDS)}) newer_than:{DIAS_BUSQUEDA}d"

# Archivo de salida para reportes
REPORT_DIR = os.path.join(BASE_DIR, "reportes")
REPORT_FILE = os.path.join(REPORT_DIR, "reporte_comparativos.txt")
REPORT_JSON = os.path.join(REPORT_DIR, "comparativos_data.json")

# Archivo de tracking de reenvios (evita duplicados)
REENVIADOS_JSON = os.path.join(REPORT_DIR, "reenviados.json")

# ============================================================
# EXCLUSIONES DE REENVIO
# Correos de estos remitentes NUNCA se reenvian a nadie
# Correos con estos patrones en asunto NUNCA se reenvian
# ============================================================
REMITENTES_EXCLUIDOS_REENVIO = [
    "alicia.conde@hergonsa.pe",
]

ASUNTOS_EXCLUIDOS_REENVIO = [
    "costo pll staff",
    "fases para cierre de mes",
    "cierre de mes",
    "renovación obra",
    "renovacion obra",
]

# ============================================================
# OBRAS / PROYECTOS
# Mapeo de palabras clave en asunto o email del remitente -> nombre de obra
# Se buscan en orden, la primera coincidencia gana.
# Agregar nuevas obras aqui segun se necesite.
# ============================================================
OBRAS = {
    "BEETHOVEN": ["beethoven", "btv"],
    "BIOMEDICAS": ["biomédica", "biomedica", "biomed"],
    "ROOSEVELT": ["roosevelt", "frankling", "franklin", "rooselvet", "roosevel"],
    "ALMA MATER": ["alma mater", "alma.mater", "mater"],
    "MARA": ["mara"],
    "CENEPA": ["cenepa"],
}


def detectar_obra(asunto, de_email=""):
    """Detecta la obra/proyecto a partir del asunto del correo o email del remitente."""
    texto = (asunto + " " + de_email).lower()
    for obra, keywords in OBRAS.items():
        for kw in keywords:
            if kw in texto:
                return obra
    return "OTROS"
