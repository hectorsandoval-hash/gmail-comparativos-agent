"""
AGENTE 1: Busqueda y Listado de Comparativos
- Busca correos en Gmail que mencionen "comparativo/comparativos"
- Extrae: asunto, de que trata, monto, resumen, remitente, fecha
- Identifica si las personas clave estan en CC/TO
"""
import base64
import re
from email.utils import parseaddr
from datetime import datetime

from config import GMAIL_SEARCH_QUERY, PERSONAS_CLAVE


def buscar_comparativos(service, max_results=50):
    """
    Busca correos que mencionen comparativos en Gmail.
    Retorna lista de diccionarios con la informacion de cada correo.
    """
    print(f"\n[AGENTE 1] Buscando correos con: '{GMAIL_SEARCH_QUERY}'")

    resultados = []
    page_token = None

    while True:
        response = (
            service.users()
            .messages()
            .list(
                userId="me",
                q=GMAIL_SEARCH_QUERY,
                maxResults=min(max_results - len(resultados), 100),
                pageToken=page_token,
            )
            .execute()
        )

        messages = response.get("messages", [])
        if not messages:
            break

        for msg_ref in messages:
            if len(resultados) >= max_results:
                break
            msg_data = _procesar_mensaje(service, msg_ref["id"])
            if msg_data:
                resultados.append(msg_data)

        page_token = response.get("nextPageToken")
        if not page_token or len(resultados) >= max_results:
            break

    print(f"[AGENTE 1] Se encontraron {len(resultados)} correos de comparativos.")
    return resultados


def _procesar_mensaje(service, message_id):
    """Procesa un mensaje individual y extrae informacion relevante."""
    msg = (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )

    headers = msg.get("payload", {}).get("headers", [])
    header_dict = {h["name"].lower(): h["value"] for h in headers}

    asunto = header_dict.get("subject", "(Sin asunto)")
    de = header_dict.get("from", "")
    para = header_dict.get("to", "")
    cc = header_dict.get("cc", "")
    fecha_raw = header_dict.get("date", "")
    message_id_header = header_dict.get("message-id", "")
    in_reply_to = header_dict.get("in-reply-to", "")
    references = header_dict.get("references", "")
    thread_id = msg.get("threadId", "")

    # Extraer cuerpo del mensaje
    cuerpo = _extraer_cuerpo(msg.get("payload", {}))

    # Extraer monto del comparativo
    texto_completo = asunto + " " + cuerpo
    monto = _extraer_monto(texto_completo)

    # Extraer PPTO META HG
    ppto_meta_hg = _extraer_ppto_meta_hg(texto_completo)

    # Generar resumen
    resumen = _generar_resumen(cuerpo, asunto)

    # Verificar si las personas clave estan en copia
    todos_destinatarios = (para + " " + cc).lower()
    personas_en_copia = _verificar_personas_en_copia(todos_destinatarios)

    # Parsear fecha
    fecha = _parsear_fecha(fecha_raw)

    # Link directo a Gmail
    gmail_link = f"https://mail.google.com/mail/u/0/#all/{msg['id']}"

    return {
        "id": msg["id"],
        "thread_id": thread_id,
        "asunto": asunto,
        "de": de,
        "de_email": parseaddr(de)[1],
        "para": para,
        "cc": cc,
        "fecha": fecha,
        "fecha_raw": fecha_raw,
        "monto": monto,
        "ppto_meta_hg": ppto_meta_hg,
        "resumen": resumen,
        "cuerpo_preview": cuerpo[:500] if cuerpo else "(sin contenido)",
        "personas_en_copia": personas_en_copia,
        "message_id_header": message_id_header,
        "in_reply_to": in_reply_to,
        "references": references,
        "labels": msg.get("labelIds", []),
        "gmail_link": gmail_link,
    }


def _extraer_cuerpo(payload):
    """Extrae el texto plano del cuerpo del mensaje."""
    cuerpo = ""

    if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
        cuerpo = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    elif payload.get("parts"):
        for part in payload["parts"]:
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
                cuerpo = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
                break
            elif part.get("mimeType", "").startswith("multipart/") and part.get("parts"):
                for subpart in part["parts"]:
                    if subpart.get("mimeType") == "text/plain" and subpart.get("body", {}).get("data"):
                        cuerpo = base64.urlsafe_b64decode(subpart["body"]["data"]).decode("utf-8", errors="replace")
                        break
                if cuerpo:
                    break

    # Si no hay texto plano, buscar HTML
    if not cuerpo:
        cuerpo = _extraer_html_como_texto(payload)

    return cuerpo.strip()


def _extraer_html_como_texto(payload):
    """Extrae texto de contenido HTML si no hay texto plano."""
    html = ""
    if payload.get("mimeType") == "text/html" and payload.get("body", {}).get("data"):
        html = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    elif payload.get("parts"):
        for part in payload["parts"]:
            if part.get("mimeType") == "text/html" and part.get("body", {}).get("data"):
                html = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
                break

    if html:
        # Remover tags HTML basicamente
        texto = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        texto = re.sub(r"<[^>]+>", " ", texto)
        texto = re.sub(r"\s+", " ", texto)
        return texto.strip()
    return ""


def _extraer_monto(texto):
    """
    Busca montos en el texto (S/, USD, $, PEN, soles, dolares).
    Solo acepta montos >= 100 para evitar falsos positivos.
    Retorna el primer monto valido encontrado o 'No especificado'.
    """
    # Patrones que capturan montos con al menos 3 digitos o con decimales significativos
    patrones = [
        # Ahorro/perdida explicito: "ahorro de S/ 14,573.669" o "pérdida de S/ -10,659.27"
        r"(?:ahorro|pérdida|perdida|ganancia)\s+de\s+(?:S/\.?|PEN)?\s*-?\s*([\d]{1,3}(?:,\d{3})*(?:\.\d+)?)",
        # S/ seguido de monto significativo (minimo 3 digitos o con coma de miles)
        r"(?:S/\.?)\s*-?\s*([\d]{1,3}(?:,\d{3})+(?:\.\d+)?)",
        r"(?:S/\.?)\s*-?\s*([\d]{3,}(?:\.\d+)?)",
        # USD/$ seguido de monto significativo
        r"(?:USD|US\$|\$)\s*-?\s*([\d]{1,3}(?:,\d{3})+(?:\.\d+)?)",
        r"(?:USD|US\$|\$)\s*-?\s*([\d]{3,}(?:\.\d+)?)",
        # Monto seguido de moneda
        r"-?\s*([\d]{1,3}(?:,\d{3})+(?:\.\d+)?)\s*(?:S/\.?|PEN|soles)",
        r"-?\s*([\d]{1,3}(?:,\d{3})+(?:\.\d+)?)\s*(?:USD|dolares|dólares)",
        # Palabras clave de monto + valor significativo
        r"(?:monto|total|precio|valor|costo|importe)[\s:]+(?:S/\.?|PEN|USD|\$)?\s*-?\s*([\d]{1,3}(?:,\d{3})+(?:\.\d+)?)",
    ]

    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            monto = match.group(1) if match.lastindex else match.group(0)
            # Verificar que el monto es significativo (>= 100)
            try:
                valor_numerico = float(monto.replace(",", ""))
                if valor_numerico < 100:
                    continue
            except ValueError:
                continue

            # Obtener contexto para saber la moneda
            inicio = max(0, match.start() - 20)
            contexto = texto[inicio : match.end() + 20].lower()

            # Detectar si es perdida
            es_perdida = any(p in contexto for p in ["pérdida", "perdida", "-"])
            signo = "-" if es_perdida and "-" in contexto else ""

            if any(m in contexto for m in ["s/", "pen", "soles"]):
                return f"S/ {signo}{monto}"
            elif any(m in contexto for m in ["usd", "$", "us$", "dolar", "dólar"]):
                return f"USD {signo}{monto}"
            return f"S/ {signo}{monto}"

    return "No especificado"


def _extraer_ppto_meta_hg(texto):
    """
    Busca el valor de PPTO META HG en el texto del correo.
    Retorna el valor encontrado o 'No especificado'.
    """
    patrones = [
        r"(?:PPTO\.?\s*META\s*HG\w*)\s*[:\s]*(?:S/\.?|PEN|USD|\$)?\s*([\d]{1,3}(?:[,.]?\d{3})*(?:\.\d+)?)",
        r"(?:PRESUPUESTO\s*META\s*HG\w*)\s*[:\s]*(?:S/\.?|PEN|USD|\$)?\s*([\d]{1,3}(?:[,.]?\d{3})*(?:\.\d+)?)",
        r"(?:PPTO\.?\s*META)\s*[:\s]*(?:S/\.?|PEN|USD|\$)?\s*([\d]{1,3}(?:[,.]?\d{3})*(?:\.\d+)?)",
        r"(?:META\s*HG)\s*[:\s]*(?:S/\.?|PEN|USD|\$)?\s*([\d]{1,3}(?:[,.]?\d{3})*(?:\.\d+)?)",
    ]

    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1)
            try:
                valor_num = float(valor.replace(",", ""))
                if valor_num >= 100:
                    return f"S/ {valor}"
            except ValueError:
                pass

    return "No especificado"


def _generar_resumen(cuerpo, asunto):
    """Genera un resumen breve del contenido del correo."""
    texto = cuerpo if cuerpo else asunto
    if not texto:
        return "Sin contenido disponible"

    # Limpiar texto
    texto = re.sub(r"[-_=]{3,}", "", texto)
    texto = re.sub(r"\s+", " ", texto).strip()

    # Tomar las primeras oraciones relevantes (hasta 300 chars)
    oraciones = re.split(r"[.!?\n]", texto)
    resumen = ""
    for oracion in oraciones:
        oracion = oracion.strip()
        if len(oracion) < 10:
            continue
        if len(resumen) + len(oracion) > 300:
            break
        resumen += oracion + ". "

    return resumen.strip() if resumen else texto[:300] + "..."


def _verificar_personas_en_copia(destinatarios_lower):
    """
    Verifica si las personas clave estan entre los destinatarios.
    Busca por variantes del nombre y por partes del email.
    """
    resultado = {}

    for key, persona in PERSONAS_CLAVE.items():
        encontrado = False
        for variante in persona["variantes_nombre"]:
            if variante in destinatarios_lower:
                encontrado = True
                break
        resultado[key] = {
            "nombre": persona["nombre"],
            "en_copia": encontrado,
        }

    return resultado


def _parsear_fecha(fecha_raw):
    """Intenta parsear la fecha del correo a formato legible."""
    formatos = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
    ]
    # Limpiar parentesis al final como (UTC) (PST) etc
    fecha_limpia = re.sub(r"\s*\([^)]*\)\s*$", "", fecha_raw).strip()

    for fmt in formatos:
        try:
            dt = datetime.strptime(fecha_limpia, fmt)
            return dt.strftime("%d/%m/%Y %H:%M")
        except ValueError:
            continue
    return fecha_raw
