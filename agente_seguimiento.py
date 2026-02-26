"""
AGENTE 3: Seguimiento de Comparativos
- Revisa cada thread/hilo de los comparativos
- Verifica si el ULTIMO mensaje que requiere un nuevo analisis fue respondido
- Mensajes de traslado, OC, confirmaciones NO cuentan como pendientes
- Solo mensajes que solicitan revision/analisis cuentan como pendientes
"""
import base64
import re
from email.utils import parseaddr

from config import PERSONAS_CLAVE, PALABRAS_NO_REQUIERE_RESPUESTA, USUARIO_NOMBRE


# Emails conocidos de personas tracked
_EMAILS_TRACKED = set()


def realizar_seguimiento(service, comparativos, mi_email):
    """Revisa el estado de respuesta de cada comparativo."""
    print("\n[AGENTE 3] Realizando seguimiento de comparativos...")

    _EMAILS_TRACKED.add(mi_email.lower())
    for _key, _persona in PERSONAS_CLAVE.items():
        _email = _persona.get("email", "")
        if _email:
            _EMAILS_TRACKED.add(_email.lower())

    seguimiento = []

    for comp in comparativos:
        thread_id = comp["thread_id"]
        estado = _analizar_thread(service, thread_id, comp, mi_email)
        seguimiento.append(estado)

    respondidos = sum(1 for s in seguimiento if s["estado_general"] == "RESPONDIDO")
    pendientes = sum(1 for s in seguimiento if s["estado_general"] == "PENDIENTE")

    print(f"\n[AGENTE 3] === RESUMEN DE SEGUIMIENTO ===")
    print(f"  Respondidos (cadena completa): {respondidos}")
    print(f"  Pendiente (requiere respuesta): {pendientes}")
    print(f"  Total: {len(seguimiento)}")

    return seguimiento


def _analizar_thread(service, thread_id, comparativo, mi_email):
    """
    Analiza un thread completo.

    Logica mejorada:
    1. Recorre TODOS los mensajes del hilo
    2. Clasifica cada mensaje:
       - "requiere_respuesta": envia comparativo nuevo, solicita revision/analisis
       - "respuesta": alguien tracked responde
       - "no_requiere": confirmacion, traslado, OC, etc.
    3. Si el ultimo mensaje que REQUIERE respuesta ya fue respondido -> RESPONDIDO
    4. Si no fue respondido -> PENDIENTE
    """
    try:
        thread = (
            service.users()
            .threads()
            .get(userId="me", id=thread_id, format="full")
            .execute()
        )
    except Exception as e:
        return {
            "id": comparativo["id"],
            "thread_id": thread_id,
            "asunto": comparativo["asunto"],
            "estado_general": "ERROR",
            "error": str(e),
            "respuestas": {},
            "total_mensajes": 0,
            "cadena_completa": False,
        }

    mensajes = thread.get("messages", [])
    total_mensajes = len(mensajes)

    respuestas = {}
    for _key, _persona in PERSONAS_CLAVE.items():
        respuestas[_key] = {
            "nombre": _persona["nombre"],
            "respondio": False,
            "fecha_respuesta": None,
        }
    respuestas["yo"] = {
        "nombre": USUARIO_NOMBRE,
        "respondio": False,
        "fecha_respuesta": None,
    }

    # Indice del ultimo mensaje que realmente REQUIERE respuesta (nuevo analisis)
    ultimo_requiere_idx = -1
    # Indice del ultimo mensaje de tracked que responde
    ultimo_tracked_idx = -1

    for idx, msg in enumerate(mensajes):
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        from_header = headers.get("from", "")
        from_email = parseaddr(from_header)[1].lower()
        from_name = parseaddr(from_header)[0].lower()
        fecha = headers.get("date", "")
        asunto = headers.get("subject", "")

        # Extraer snippet/cuerpo breve para clasificar
        snippet = msg.get("snippet", "").lower()

        es_tracked = False

        # Verificar si es del usuario
        if from_email == mi_email.lower():
            es_tracked = True
            respuestas["yo"]["respondio"] = True
            respuestas["yo"]["fecha_respuesta"] = fecha

        # Verificar personas clave
        for key, persona in PERSONAS_CLAVE.items():
            for variante in persona["variantes_nombre"]:
                if variante in from_name or variante in from_email:
                    es_tracked = True
                    respuestas[key]["respondio"] = True
                    respuestas[key]["fecha_respuesta"] = fecha
                    break

        if es_tracked:
            ultimo_tracked_idx = idx
        else:
            # Mensaje de alguien externo - clasificar si requiere respuesta
            if _mensaje_requiere_respuesta(snippet, asunto):
                ultimo_requiere_idx = idx
            # Si es traslado/OC/confirmacion, NO cuenta como pendiente

    # Determinar estado
    if total_mensajes <= 1:
        # Solo 1 mensaje
        first_from = ""
        if mensajes:
            first_headers = {h["name"].lower(): h["value"] for h in mensajes[0].get("payload", {}).get("headers", [])}
            first_from = parseaddr(first_headers.get("from", ""))[1].lower()

        if first_from == mi_email.lower():
            estado_general = "RESPONDIDO"
        else:
            estado_general = "PENDIENTE"
    elif ultimo_requiere_idx == -1:
        # No hay mensajes que requieran respuesta (todo es confirmacion/traslado)
        estado_general = "RESPONDIDO"
    elif ultimo_tracked_idx > ultimo_requiere_idx:
        # Alguien tracked respondio despues del ultimo que requiere respuesta
        estado_general = "RESPONDIDO"
    else:
        # El ultimo mensaje que requiere respuesta no ha sido respondido
        estado_general = "PENDIENTE"

    # Determinar "en cancha de quien"
    # Logica: si el usuario NO ha respondido a la cadena → esta en su cancha
    #         si ya respondio (o alguien tracked respondio) → CERRADO
    if estado_general == "RESPONDIDO":
        en_cancha_de = "CERRADO"
    else:
        # PENDIENTE: el ultimo msg que requiere respuesta no fue atendido
        # Si el usuario no ha respondido → esta en su cancha
        en_cancha_de = USUARIO_NOMBRE.upper()

    return {
        "id": comparativo["id"],
        "thread_id": thread_id,
        "asunto": comparativo["asunto"],
        "de": comparativo["de"],
        "fecha": comparativo["fecha"],
        "monto": comparativo["monto"],
        "estado_general": estado_general,
        "en_cancha_de": en_cancha_de,
        "respuestas": respuestas,
        "total_mensajes": total_mensajes,
        "cadena_completa": ultimo_tracked_idx > ultimo_requiere_idx,
    }


def _mensaje_requiere_respuesta(snippet, asunto):
    """
    Determina si un mensaje requiere una nueva respuesta/revision.

    NO requiere respuesta (es confirmacion/traslado):
    - "de acuerdo", "ok", "proceder", "aprobado"
    - "generar OC", "orden de compra", "adjunto OC"
    - "se traslada", "traslado"

    SI requiere respuesta (nuevo analisis):
    - "adjunto comparativo", "cuadro comparativo"
    - "revision", "evaluar", "analizar"
    - Tiene adjuntos Excel nuevos
    """
    texto = (snippet + " " + asunto).lower()

    # Verificar si es una confirmacion/traslado (NO requiere respuesta)
    for palabra in PALABRAS_NO_REQUIERE_RESPUESTA:
        if palabra in texto:
            return False

    # Si llego aqui, por defecto SI requiere respuesta
    # (es un mensaje nuevo de alguien externo que no es confirmacion)
    return True
