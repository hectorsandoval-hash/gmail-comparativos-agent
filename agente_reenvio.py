"""
AGENTE 2: Verificacion de CC y Reenvio
- Revisa la lista de comparativos del Agente 1
- Identifica correos donde las personas clave NO estan en copia
- Excluye al remitente (si la persona envio el correo, no se le reenvia)
- Lleva tracking de reenvios para evitar duplicados entre ejecuciones
- Reenvia el correo COMPLETO (cuerpo + adjuntos) SOLO a la persona que falta
"""
import base64
import json
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

from config import PERSONAS_CLAVE, REPORT_DIR, REENVIADOS_JSON, REMITENTES_EXCLUIDOS_REENVIO, ASUNTOS_EXCLUIDOS_REENVIO


def _cargar_reenviados():
    """Carga el registro de correos ya reenviados."""
    if os.path.exists(REENVIADOS_JSON):
        try:
            with open(REENVIADOS_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _guardar_reenviados(reenviados):
    """Guarda el registro de correos reenviados."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(REENVIADOS_JSON, "w", encoding="utf-8") as f:
        json.dump(reenviados, f, ensure_ascii=False, indent=2)


def analizar_y_reenviar(service, comparativos, mi_email, auto_reenviar=False):
    """
    Analiza los comparativos y reenvia los que faltan a las personas clave.
    Excluye al remitente y evita reenvios duplicados.

    Args:
        service: Gmail API service
        comparativos: Lista de comparativos del Agente 1
        mi_email: Email del usuario autenticado
        auto_reenviar: Si True, reenvia sin pedir confirmacion (False por defecto)

    Returns:
        dict con resultados del reenvio
    """
    print("\n[AGENTE 2] Analizando destinatarios de comparativos...")

    # Cargar registro de reenvios previos
    reenviados = _cargar_reenviados()

    correos_sin_copia = []
    correos_ok = []

    for comp in comparativos:
        de_email = comp.get("de_email", "").lower()
        asunto_lower = comp.get("asunto", "").lower()

        # Excluir por remitente
        if de_email in [r.lower() for r in REMITENTES_EXCLUIDOS_REENVIO]:
            print(f"  [EXCLUIDO] Remitente excluido ({de_email}): '{comp['asunto'][:50]}'")
            correos_ok.append(comp)
            continue

        # Excluir por patron en asunto
        excluido_asunto = False
        for patron in ASUNTOS_EXCLUIDOS_REENVIO:
            if patron in asunto_lower:
                print(f"  [EXCLUIDO] Asunto excluido ('{patron}'): '{comp['asunto'][:50]}'")
                correos_ok.append(comp)
                excluido_asunto = True
                break
        if excluido_asunto:
            continue

        faltantes = []
        faltantes_emails = []

        for key, info in comp["personas_en_copia"].items():
            persona_config = PERSONAS_CLAVE.get(key, {})
            email_persona = persona_config.get("email", "")

            # Excluir al remitente: si la persona envio el correo, no es faltante
            if email_persona and de_email == email_persona.lower():
                continue

            if not info["en_copia"]:
                # Verificar si ya fue reenviado a esta persona
                reenvio_key = f"{comp['id']}_{email_persona}"
                if reenvio_key in reenviados:
                    print(f"  [SKIP] Ya reenviado: '{comp['asunto'][:40]}' a {email_persona}")
                    continue

                faltantes.append(info["nombre"])
                faltantes_emails.append(email_persona)

        if faltantes:
            correos_sin_copia.append({
                "comparativo": comp,
                "faltantes": faltantes,
                "faltantes_emails": faltantes_emails,
            })
        else:
            correos_ok.append(comp)

    print(f"[AGENTE 2] Correos con todos en copia: {len(correos_ok)}")
    print(f"[AGENTE 2] Correos que necesitan reenvio: {len(correos_sin_copia)}")

    resultados_reenvio = []

    if correos_sin_copia:
        print("\n" + "=" * 60)
        print("CORREOS QUE NECESITAN REENVIO:")
        print("=" * 60)

        for i, item in enumerate(correos_sin_copia, 1):
            comp = item["comparativo"]
            faltantes = item["faltantes"]
            print(f"\n  {i}. Asunto: {comp['asunto']}")
            print(f"     De: {comp['de']}")
            print(f"     Fecha: {comp['fecha']}")
            print(f"     Faltantes: {', '.join(faltantes)}")

        if auto_reenviar:
            print("\n[AGENTE 2] Modo automatico activado. Reenviando...")
            for item in correos_sin_copia:
                resultado = _reenviar_correo(
                    service, item["comparativo"], mi_email,
                    item["faltantes_emails"], reenviados
                )
                resultados_reenvio.append(resultado)

            # Guardar registro actualizado
            _guardar_reenviados(reenviados)
        else:
            print("\n[AGENTE 2] Los correos listados arriba necesitan ser reenviados.")
            print("[AGENTE 2] Ejecuta con auto_reenviar=True para reenviar automaticamente.")

    return {
        "correos_ok": correos_ok,
        "correos_sin_copia": correos_sin_copia,
        "resultados_reenvio": resultados_reenvio,
    }


def reenviar_correo_individual(service, comparativo, mi_email):
    """Reenvia un correo individual a las personas clave faltantes."""
    reenviados = _cargar_reenviados()
    de_email = comparativo.get("de_email", "").lower()
    destinatarios = []

    for key, info in comparativo["personas_en_copia"].items():
        persona_config = PERSONAS_CLAVE.get(key, {})
        email_persona = persona_config.get("email", "")

        if email_persona and de_email == email_persona.lower():
            continue

        if not info["en_copia"]:
            reenvio_key = f"{comparativo['id']}_{email_persona}"
            if reenvio_key not in reenviados:
                destinatarios.append(email_persona)

    if not destinatarios:
        return {"id": comparativo["id"], "estado": "YA_REENVIADO"}

    resultado = _reenviar_correo(service, comparativo, mi_email, destinatarios, reenviados)
    _guardar_reenviados(reenviados)
    return resultado


def _obtener_cuerpo_completo(payload):
    """
    Extrae el cuerpo completo del mensaje (texto plano y HTML).
    Busca recursivamente en todas las partes del mensaje.
    """
    texto = ""
    html = ""

    def _buscar(parte):
        nonlocal texto, html
        mime = parte.get("mimeType", "")
        body_data = parte.get("body", {}).get("data", "")
        filename = parte.get("filename", "")

        # Solo procesar partes de texto sin filename (no son adjuntos)
        if not filename and body_data:
            decoded = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
            if mime == "text/plain" and not texto:
                texto = decoded
            elif mime == "text/html" and not html:
                html = decoded

        # Buscar en sub-partes
        for sub in parte.get("parts", []):
            _buscar(sub)

    _buscar(payload)
    return texto, html


def _obtener_adjuntos(service, message_id, payload):
    """
    Descarga TODOS los archivos adjuntos del mensaje original.
    Maneja adjuntos grandes (con attachmentId) y pequenos (inline data).
    """
    adjuntos = []

    def _buscar_adjuntos(parte):
        filename = parte.get("filename", "")
        body = parte.get("body", {})
        mime_type = parte.get("mimeType", "application/octet-stream")

        if filename:
            # Es un adjunto
            data = None

            if body.get("attachmentId"):
                # Adjunto grande: descargar via API
                try:
                    att_response = (
                        service.users()
                        .messages()
                        .attachments()
                        .get(userId="me", messageId=message_id, id=body["attachmentId"])
                        .execute()
                    )
                    data = base64.urlsafe_b64decode(att_response["data"])
                except Exception as e:
                    print(f"    [WARN] No se pudo descargar adjunto '{filename}': {e}")
            elif body.get("data"):
                # Adjunto pequeno: datos inline
                data = base64.urlsafe_b64decode(body["data"])

            if data:
                maintype, _, subtype = mime_type.partition("/")
                adjuntos.append({
                    "filename": filename,
                    "data": data,
                    "maintype": maintype or "application",
                    "subtype": subtype or "octet-stream",
                    "size": len(data),
                })

        # Buscar en sub-partes
        for sub in parte.get("parts", []):
            _buscar_adjuntos(sub)

    _buscar_adjuntos(payload)
    return adjuntos


def _reenviar_correo(service, comparativo, mi_email, destinatarios_emails, reenviados):
    """
    Reenvia un correo COMPLETO (cuerpo + adjuntos) SOLO a las personas que faltan en CC.
    Descarga el mensaje original con todos sus adjuntos y los incluye en el reenvio.
    Registra el reenvio para evitar duplicados.
    """
    asunto_original = comparativo["asunto"]
    message_id = comparativo["id"]

    # Filtrar destinatarios vacios
    destinatarios_reenvio = [e for e in destinatarios_emails if e]

    if not destinatarios_reenvio:
        return {
            "id": message_id,
            "asunto": asunto_original,
            "estado": "NO_ENVIADO",
            "motivo": "No se encontraron emails de destinatarios",
        }

    try:
        # === PASO 1: Obtener el mensaje original COMPLETO ===
        print(f"    Descargando mensaje original completo...")
        msg_original = (
            service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
            .execute()
        )
        payload = msg_original.get("payload", {})

        # === PASO 2: Extraer cuerpo completo (texto + HTML) ===
        cuerpo_texto, cuerpo_html = _obtener_cuerpo_completo(payload)

        # === PASO 3: Descargar todos los adjuntos ===
        adjuntos = _obtener_adjuntos(service, message_id, payload)
        if adjuntos:
            print(f"    Adjuntos encontrados: {len(adjuntos)} ({', '.join(a['filename'] for a in adjuntos)})")

        # === PASO 4: Construir mensaje de reenvio completo ===
        msg = MIMEMultipart("mixed")
        msg["to"] = ", ".join(destinatarios_reenvio)
        msg["from"] = mi_email
        msg["subject"] = f"Fwd: {asunto_original}"

        # Header de reenvio
        header_info = (
            f"De: {comparativo['de']}\n"
            f"Fecha: {comparativo['fecha']}\n"
            f"Asunto: {asunto_original}\n"
            f"Para: {comparativo['para']}\n"
            f"CC: {comparativo['cc']}"
        )

        if cuerpo_html:
            # Si hay HTML, enviar como HTML con header
            header_html = (
                '<div style="font-family:Arial,sans-serif;font-size:12px;color:#666;">'
                '---------- Mensaje reenviado ----------<br>'
                f'De: {comparativo["de"]}<br>'
                f'Fecha: {comparativo["fecha"]}<br>'
                f'Asunto: {asunto_original}<br>'
                f'Para: {comparativo["para"]}<br>'
                f'CC: {comparativo["cc"]}<br>'
                '----------------------------------------</div><br><hr><br>'
            )
            html_completo = header_html + cuerpo_html
            msg.attach(MIMEText(html_completo, "html", "utf-8"))
        elif cuerpo_texto:
            # Si solo hay texto plano
            texto_completo = (
                f"---------- Mensaje reenviado ----------\n"
                f"{header_info}\n"
                f"----------------------------------------\n\n"
                f"{cuerpo_texto}"
            )
            msg.attach(MIMEText(texto_completo, "plain", "utf-8"))
        else:
            # Fallback: usar cuerpo_preview del comparativo
            texto_fallback = (
                f"---------- Mensaje reenviado ----------\n"
                f"{header_info}\n"
                f"----------------------------------------\n\n"
                f"{comparativo.get('cuerpo_preview', '(sin contenido)')}"
            )
            msg.attach(MIMEText(texto_fallback, "plain", "utf-8"))

        # === PASO 5: Adjuntar TODOS los archivos ===
        for adjunto in adjuntos:
            att = MIMEBase(adjunto["maintype"], adjunto["subtype"])
            att.set_payload(adjunto["data"])
            encoders.encode_base64(att)
            att.add_header(
                "Content-Disposition",
                "attachment",
                filename=adjunto["filename"],
            )
            msg.attach(att)

        # === PASO 6: Enviar ===
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

        sent = (
            service.users()
            .messages()
            .send(userId="me", body={"raw": raw_message})
            .execute()
        )

        adjuntos_info = f" + {len(adjuntos)} adjunto(s)" if adjuntos else ""
        print(f"  [OK] Reenviado: '{asunto_original[:45]}' a {', '.join(destinatarios_reenvio)}{adjuntos_info}")

        # Registrar en tracking
        for email_dest in destinatarios_reenvio:
            reenvio_key = f"{comparativo['id']}_{email_dest}"
            reenviados[reenvio_key] = {
                "asunto": asunto_original,
                "reenviado_a": email_dest,
                "fecha": datetime.now().isoformat(),
                "gmail_id": sent["id"],
                "adjuntos": [a["filename"] for a in adjuntos],
            }

        return {
            "id": message_id,
            "asunto": asunto_original,
            "estado": "ENVIADO",
            "destinatarios": destinatarios_reenvio,
            "gmail_id": sent["id"],
            "adjuntos": len(adjuntos),
        }
    except Exception as e:
        print(f"  [ERROR] No se pudo reenviar '{asunto_original}': {e}")
        return {
            "id": message_id,
            "asunto": asunto_original,
            "estado": "ERROR",
            "error": str(e),
        }
