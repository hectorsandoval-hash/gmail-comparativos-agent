"""
Envia un correo resumen del estatus de comparativos al usuario.
Filtra correos que NO son comparativos (valorizaciones, OC, etc.)
Incluye: montos, PPTO META HG, hipervinculo a Gmail, en cancha de quien
Agrupa comparativos por OBRA/PROYECTO.

Destinatarios configurados via variables de entorno (GitHub Secrets).
MODO_PRUEBA: Si esta activo en config.py, solo envia al usuario (no a otros)
"""
import json
import re
import base64
from collections import OrderedDict
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta

# Zona horaria Peru (UTC-5)
PERU_TZ = timezone(timedelta(hours=-5))

import os

from auth_gmail import autenticar_gmail, obtener_perfil
from config import REPORT_JSON, MODO_PRUEBA, detectar_obra, OBRAS, PERSONAS_CLAVE, USUARIO_NOMBRE

# Remitentes cuyos correos se ignoran completamente en el analisis
# Se leen de variable de entorno EXCLUIR_REMITENTES_JSON (GitHub Secret)
_excluir_json = os.environ.get("EXCLUIR_REMITENTES_JSON", "")
EXCLUIR_REMITENTES = json.loads(_excluir_json) if _excluir_json else []

# Palabras en asunto que indican que NO es un comparativo real
EXCLUIR_ASUNTOS = [
    "valorizacion",
    "valorización",
    "reestructuración de directorio",
    "reestructuracion de directorio",
    "[reporte] estatus de comparativos",
    # Cronogramas de adquisiciones (no son comparativos)
    "cronograma de adquisicion",
    "cronograma de adquisiciones",
    "cronograma adquisicion",
    # Contratos y ordenes (no son comparativos)
    "contrato",
    "contratos",
    "ordenes firmadas",
    "orden firmada",
    "ordenes de compra firmadas",
    # Otros correos que no son comparativos
    "entrevista",
    "entrevistas",
    "informacion sub contratista",
    "información sub contratista",
    "sub contratista",
    # Correos administrativos que no son comparativos
    "fases para cierre de mes",
    "cierre de mes",
    "costo pll staff",
    "renovación obra",
    "renovacion obra",
    # RRHH / planillas / personal
    "ingreso personal",
    "ingreso de personal",
    "sueldo",
    "sueldos",
    "planilla",
    "planillas",
    "staff",
    "pll",
    "remuneracion",
    "remuneración",
    "boleta de pago",
    "liquidacion",
    "liquidación",
    # Oficios, invitaciones y documentos administrativos
    "oficio",
    "invitación proceso",
    "invitacion proceso",
    # Notas de reuniones (Google Meet / Gemini)
    "notas:",
    "notas de",
    # Correos internos que no son comparativos
    "vgu",
    # Facturas y pagos (no son comparativos)
    "facturas vencidas",
    "facturas por vencer",
    "deudas pendientes",
    # Reportes administrativos
    "reporte hh",
    # Autorizacion de personal
    "autorizacion de personal",
    "autorización de personal",
    # Envio de OC (ya no es revision de comparativo)
    "envio oc",
    "envío oc",
    # Informes y fichas tecnicas
    "presentacion de ficha",
    "presentación de ficha",
    # Equipamiento y muestras (no son comparativos)
    "equipamiento tecnologico",
    "equipamiento tecnológico",
    "recojo de muestras",
    # Pagos y OC existentes (tramite, no comparativo)
    "cema ",
    # Proyectados y valorizaciones
    "proyectado marzo",
    "proyectado febrero",
    "proyectado enero",
    "proyectado abril",
    "proyectado mayo",
    "proyectado junio",
    "val proyectada",
    "val semanal",
    "modelo de val",
    "conformidad supervision",
    "conformidad supervisión",
    # Documentos de procura (entrega de carpetas, no comparativo)
    "documentos de procura",
    # Combustible (no es comparativo)
    "combustible",
    # OC ya generadas o aprobaciones de compra (tramite cerrado)
    "oc de proyector",
    "generar oc",
    "aprobacion de compra",
    "aprobación de compra",
    "oc generada",
    # Tableros electricos / envio de RQ (requerimiento, no comparativo)
    "tablero electricos",
    "tableros electricos",
    # Cortes y cuadros internos de OC/cotizaciones
    "oc si/no",
]

# Palabras en asunto que requieren revision del cuerpo para confirmar
# Si el asunto dice REQUERIMIENTO/REQ pero el cuerpo NO menciona
# revision/aprobacion de comparativo, se excluye (es solo logistica).
PALABRAS_REQ = ["requerimiento", "requerimientos"]

# Palabras en el cuerpo que confirman que un REQ SI es un comparativo real
# Solo incluye terminos ESPECIFICOS de comparativos/revision/aprobacion.
# Palabras genericas como "proveedor", "adjunto", "monto", "precio" se
# eliminaron porque aparecen en cualquier correo de procura/logistica.
PALABRAS_CONFIRMA_COMPARATIVO = [
    "comparativo", "cuadro comparativo", "c.c.", "cc.",
    "revisión", "revision", "revisar",
    "aprobación", "aprobacion", "aprobar",
    "evaluar", "analizar", "analisis", "análisis",
    "cotización", "cotizacion", "cotizaciones",
    "cuadro",
]

# Destinatarios adicionales (se leen de variables de entorno / GitHub Secrets)
# config.py carga .env local automaticamente si existe
import os as _os
_dest_cf = _os.environ.get("DESTINATARIOS_CON_FALTANTES", "")
DESTINATARIOS_CON_FALTANTES = [e.strip() for e in _dest_cf.split(",") if e.strip()] if _dest_cf else []
_dest_sf = _os.environ.get("DESTINATARIOS_SIN_FALTANTES", "")
DESTINATARIOS_SIN_FALTANTES = [e.strip() for e in _dest_sf.split(",") if e.strip()] if _dest_sf else []


def _es_req_sin_comparativo(comp):
    """Verifica si un correo de REQUERIMIENTO/REQ es solo logistica (no comparativo).

    Si el asunto dice REQUERIMIENTO/REQ pero el cuerpo NO menciona
    revision/aprobacion de comparativo, se entiende que solo se esta
    mandando al area de logistica para cotizar y armar el cuadro comparativo.
    """
    asunto_lower = comp["asunto"].lower()

    # Verificar si el asunto contiene REQUERIMIENTO o REQ
    tiene_req = False
    for palabra_req in PALABRAS_REQ:
        if palabra_req in asunto_lower:
            tiene_req = True
            break
    # Tambien detectar "REQ" como abreviatura (con espacio/inicio)
    if not tiene_req:
        # Buscar "req " o "req." como abreviatura al inicio o despues de espacio
        if re.search(r'(?:^|\s|-)req(?:\s|\.|\d|$)', asunto_lower):
            tiene_req = True

    if not tiene_req:
        return False  # No es un correo de REQ

    # Es un REQ: pero si el asunto tiene keyword de OBRA, es de un proyecto
    # real y NO debe excluirse (ej: "BTV : REQUERIMIENTO N° 39" → BEETHOVEN)
    for obra, keywords in OBRAS.items():
        for kw in keywords:
            if kw in asunto_lower:
                return False  # Tiene obra identificada → NO excluir

    # Revisar AMBOS campos (cuerpo_preview + resumen + asunto) para
    # ver si en alguno se menciona algo de comparativo
    cuerpo = " ".join([
        comp.get("cuerpo_preview", "") or "",
        comp.get("resumen", "") or "",
        comp.get("asunto", "") or "",
    ]).lower()
    for palabra_conf in PALABRAS_CONFIRMA_COMPARATIVO:
        if palabra_conf in cuerpo:
            return False  # SI es un comparativo real, NO excluir

    # El cuerpo no menciona comparativo y no tiene obra → es solo logistica → EXCLUIR
    return True


def _normalizar_asunto(asunto):
    """Normaliza un asunto removiendo prefijos Fwd:/Re:/RE:/RV: para deduplicacion.

    RV: es el prefijo de Outlook en espanol para reenvios (equivalente a Fwd:).

    Ejemplo: 'Fwd: Re: CC. BEETHOVEN TR4' → 'cc. beethoven tr4'
    Ejemplo: 'RV: SE REMITE CUADRO COMPARATIVO' → 'se remite cuadro comparativo'
    """
    return re.sub(r'^(fwd:\s*|re:\s*|rv:\s*)+', '', asunto.strip(), flags=re.IGNORECASE).strip().lower()


def _deduplicar_comparativos(comparativos):
    """Elimina duplicados por asunto normalizado.

    Cuando hay un correo original (Re:) y su version reenviada (Fwd:),
    se prefiere el original porque tiene el hilo completo de seguimiento.
    Si solo existe la version Fwd (el original es mas antiguo), se mantiene.
    """
    grupos = {}
    for comp in comparativos:
        key = _normalizar_asunto(comp["asunto"])
        if key not in grupos:
            grupos[key] = []
        grupos[key].append(comp)

    unicos = []
    duplicados_total = 0

    for key, grupo in grupos.items():
        if len(grupo) == 1:
            unicos.append(grupo[0])
        else:
            # Separar: versiones originales (Re:) vs reenviadas (Fwd:)
            originales = [c for c in grupo if not c["asunto"].lower().startswith("fwd:")]
            fwd_only = [c for c in grupo if c["asunto"].lower().startswith("fwd:")]

            if originales:
                # Mantener el original con mas mensajes en el hilo
                mejor = max(originales, key=lambda c: c.get("seguimiento", {}).get("total_mensajes_hilo", 0))
                unicos.append(mejor)
            else:
                # Solo hay Fwd: (el original esta fuera del rango de busqueda)
                # Mantener el primero (mas reciente)
                unicos.append(fwd_only[0])

            duplicados_total += len(grupo) - 1

    if duplicados_total:
        print(f"  [DEDUP] Se eliminaron {duplicados_total} correos duplicados por asunto")

    return unicos


def _asunto_tiene_termino_comparativo(asunto):
    """Verifica si el asunto contiene terminos que indican que es un comparativo.

    Terminos: "comparativo", "comparativos", "compartivo" (typo comun),
    "c.c.", y "CC"/"CC."/"CC:" como abreviatura de Cuadro Comparativo.
    """
    asunto_lower = asunto.lower()

    # Terminos directos
    for termino in ["comparativo", "comparativos", "compartivo", "c.c."]:
        if termino in asunto_lower:
            return True

    # "CC" como abreviatura de Cuadro Comparativo
    # Puede aparecer al inicio (despues de Re:/Fwd:) o despues de "/ " o "- "
    asunto_norm = re.sub(r'^(fwd:\s*|re:\s*|rv:\s*)+', '', asunto_lower).strip()
    if re.search(r'(?:^|[/\-]\s*)cc[\s.:]', asunto_norm):
        return True

    return False


def _es_cc_only_no_comparativo(comp, mi_email):
    """Verifica si el usuario esta solo en CC (no en TO) y el correo NO es
    un comparativo para su area.

    Si el usuario esta solo en CC y el asunto no contiene terminos de
    comparativo (CC., comparativo, c.c.), es probable que el correo
    sea de otra area y el usuario solo esta copiado para informacion.

    Aplica solo a correos tipo REQ/REQUERIMIENTO/COTIZACION REQ.
    """
    if not mi_email:
        return False

    # Verificar si el usuario esta en TO (no solo en CC)
    para = comp.get("para", "").lower()
    if mi_email.lower() in para:
        return False  # Esta en TO → es para el usuario → NO excluir

    # El usuario esta solo en CC. Verificar si tiene termino de comparativo
    if _asunto_tiene_termino_comparativo(comp["asunto"]):
        return False  # Tiene termino de comparativo → es de su area → NO excluir

    # Solo en CC y sin termino de comparativo:
    # Verificar si el asunto es un REQ/REQUERIMIENTO/COTIZACION REQ
    asunto_lower = comp["asunto"].lower()
    tiene_req = any(p in asunto_lower for p in PALABRAS_REQ)
    if not tiene_req:
        tiene_req = bool(re.search(r'(?:^|\s|-)req(?:\s|\.|\d|$)', asunto_lower))
    tiene_cotizacion_req = "cotizacion req" in asunto_lower or "cotización req" in asunto_lower

    if tiene_req or tiene_cotizacion_req:
        return True  # CC-only + REQ sin comparativo → EXCLUIR

    return False


def filtrar_comparativos(comparativos, mi_email=""):
    """Filtra correos que no son comparativos reales.

    Pasos:
    1. Excluir por remitente (EXCLUIR_REMITENTES)
    2. Excluir por palabras clave en asunto (EXCLUIR_ASUNTOS)
    3. Excluir REQUERIMIENTO/REQ que no mencionan comparativo en el cuerpo
    4. Excluir correos donde el usuario esta solo en CC y es un REQ/COTIZACION
       sin terminos de comparativo (no es de su area)
    5. Deduplicar por asunto normalizado (Fwd:/Re: del mismo correo)

    NOTA: No se excluyen Fwd: del usuario de forma automatica.
    Se usa deduplicacion inteligente: si el original Y el Fwd existen,
    se queda el original. Si solo existe el Fwd, se mantiene.
    """
    filtrados = []
    excluidos = []

    for comp in comparativos:
        asunto_lower = comp["asunto"].lower()

        es_excluido = False

        # 1. Excluir por remitente
        de_email_lower = comp.get("de_email", "").lower()
        for remitente in EXCLUIR_REMITENTES:
            if remitente.lower() == de_email_lower:
                es_excluido = True
                break

        # 2. Excluir por palabras clave en asunto
        if not es_excluido:
            for palabra in EXCLUIR_ASUNTOS:
                if palabra in asunto_lower:
                    es_excluido = True
                    break

        # 3. Excluir REQUERIMIENTO/REQ que no son comparativos (solo logistica)
        if not es_excluido and _es_req_sin_comparativo(comp):
            es_excluido = True

        # 4. Excluir correos donde usuario esta solo en CC y es REQ sin comparativo
        if not es_excluido and _es_cc_only_no_comparativo(comp, mi_email):
            es_excluido = True

        if es_excluido:
            excluidos.append(comp)
        else:
            filtrados.append(comp)

    # 5. Deduplicar por asunto normalizado (Fwd:/Re: del mismo correo → queda 1)
    filtrados = _deduplicar_comparativos(filtrados)

    return filtrados, excluidos


def _agrupar_por_obra(comparativos):
    """Agrupa los comparativos por obra/proyecto y los ordena."""
    grupos = OrderedDict()
    for comp in comparativos:
        obra = comp.get("obra", detectar_obra(comp.get("asunto", ""), comp.get("de_email", "")))
        if obra not in grupos:
            grupos[obra] = []
        grupos[obra].append(comp)

    # Mover "OTROS" al final si existe
    if "OTROS" in grupos:
        otros = grupos.pop("OTROS")
        grupos["OTROS"] = otros

    return grupos


def generar_cuerpo_email(comparativos, mi_email, incluir_ver=True, incluir_faltantes=True):
    """Genera el cuerpo HTML del correo resumen.

    Args:
        comparativos: Lista de comparativos filtrados
        mi_email: Email del usuario
        incluir_ver: Si True, incluye columna "Ver" con link a Gmail
        incluir_faltantes: Si True, incluye tabla 2 (faltantes CC)
    """

    # Separar por estado
    cerrados = [c for c in comparativos if c["seguimiento"].get("en_cancha_de", "") == "CERRADO"]
    en_cancha = [c for c in comparativos if c["seguimiento"].get("en_cancha_de", "") != "CERRADO"]

    # Numero de columnas (para colspan de agrupacion)
    num_cols = 13 if incluir_ver else 12

    html = f"""
<html>
<head>
<style>
  body {{ font-family: Arial, sans-serif; font-size: 12px; color: #333; }}
  h2 {{ color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 5px; }}
  h3 {{ color: #333; margin-top: 25px; margin-bottom: 8px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
  th {{ background-color: #1a73e8; color: white; padding: 7px 8px; text-align: left; font-size: 11px; white-space: nowrap; }}
  td {{ border: 1px solid #ddd; padding: 5px 8px; font-size: 11px; }}
  tr:nth-child(even) {{ background-color: #f9f9f9; }}
  .verde {{ color: #0d8043; font-weight: bold; }}
  .rojo {{ color: #d93025; font-weight: bold; }}
  .resumen-box {{ background: #e8f0fe; border-left: 4px solid #1a73e8; padding: 12px; margin: 15px 0; }}
  .footer {{ color: #888; font-size: 10px; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 10px; }}
  a.ver-correo {{ color: #1a73e8; text-decoration: none; font-weight: bold; }}
  a.ver-correo:hover {{ text-decoration: underline; }}
  .badge-cerrado {{ background: #e6f4ea; color: #0d8043; padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }}
  .badge-cancha {{ background: #fce8e6; color: #d93025; padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }}
  .badge-remitente {{ background: #e0e0e0; color: #666; padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }}
  .obra-header {{ font-weight: bold; padding: 8px 10px; font-size: 12px; text-align: left; border: 1px solid #ddd; border-left: 4px solid; }}
  .obra-BEETHOVEN {{ background: #fff8e1; color: #e65100; border-left-color: #ff8f00; }}
  .obra-BIOMEDICAS {{ background: #e0f2f1; color: #00695c; border-left-color: #00897b; }}
  .obra-ROOSEVELT {{ background: #ede7f6; color: #4527a0; border-left-color: #7e57c2; }}
  .obra-ALMA_MATER {{ background: #e3f2fd; color: #1565c0; border-left-color: #1e88e5; }}
  .obra-MARA {{ background: #fce4ec; color: #c62828; border-left-color: #e53935; }}
  .obra-CENEPA {{ background: #e8f5e9; color: #2e7d32; border-left-color: #43a047; }}
  .obra-OTROS {{ background: #e0e0e0; color: #424242; border-left-color: #757575; }}
</style>
</head>
<body>

<h2>REPORTE DE ESTATUS - COMPARATIVOS</h2>
<p>Fecha: <strong>{datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}</strong></p>

<div class="resumen-box">
  <strong>RESUMEN EJECUTIVO</strong><br>
  Total de comparativos: <strong>{len(comparativos)}</strong><br>
  Cerrados (cadena completa): <span class="verde">{len(cerrados)}</span><br>
  Pendientes de respuesta: <span class="rojo">{len(en_cancha)}</span>
</div>

<h3>1. SEGUIMIENTO COMPLETO DE COMPARATIVOS</h3>
<p style="font-size:11px; color:#666;">"Pdte. Rpta." = Pendiente de Respuesta por (quien debe actuar). CERRADO = cadena atendida.</p>
<table>
  <tr>
    <th>#</th>
    <th>Asunto</th>
    <th>De</th>
    <th>Fecha</th>
    <th>EXPEDIENTE</th>
    <th>Monto CC</th>
    <th>PPTO META HG</th>
"""
    # Columnas dinamicas por persona clave
    for _key, _persona in PERSONAS_CLAVE.items():
        _nombre = _persona["nombre"]
        # Abreviar: "Nombre Apellido" → "N.Apellido"
        _partes = _nombre.split()
        _abrev = f"{_partes[0][0]}.{_partes[-1]}" if len(_partes) > 1 else _nombre
        html += f"""
    <th>{_abrev}</th>"""
    html += f"""
    <th>{USUARIO_NOMBRE}</th>
    <th>Msgs</th>
    <th>Pdte. Rpta.</th>"""

    if incluir_ver:
        html += """
    <th>Ver</th>"""

    html += """
  </tr>
"""

    # Agrupar por obra
    grupos = _agrupar_por_obra(comparativos)
    contador = 0

    for obra, comps_obra in grupos.items():
        # Clase CSS por obra (reemplazar espacios con _)
        obra_css = obra.replace(" ", "_")
        html += f"""  <tr>
    <td class="obra-header obra-{obra_css}" colspan="{num_cols}">OBRA: {obra} ({len(comps_obra)} comparativo{"s" if len(comps_obra) != 1 else ""})</td>
  </tr>
"""
        for comp in comps_obra:
            contador += 1
            seg = comp["seguimiento"]
            # Generar columnas de respuesta dinamicamente por persona clave
            _personas_resp_html = ""
            for _key in PERSONAS_CLAVE:
                _respondio = seg.get(f"{_key}_respondio", False)
                _personas_resp_html += '<td>' + ('<span class="verde">SI</span>' if _respondio else '<span class="rojo">NO</span>') + '</td>\n    '
            yo_resp = '<span class="verde">SI</span>' if seg["yo_respondi"] else '<span class="rojo">NO</span>'

            en_cancha_de = seg.get("en_cancha_de", "PENDIENTE")
            if en_cancha_de == "CERRADO":
                cancha_fmt = '<span class="badge-cerrado">CERRADO</span>'
            else:
                cancha_fmt = f'<span class="badge-cancha">{en_cancha_de}</span>'

            monto = comp.get("monto", "No especificado")
            expediente = comp.get("expediente", "No especificado")
            ppto = comp.get("ppto_meta_hg", "No especificado")
            gmail_link = comp.get("gmail_link", "#")

            html += f"""  <tr>
    <td>{contador}</td>
    <td>{comp['asunto'][:55]}</td>
    <td>{comp['de_email']}</td>
    <td>{comp['fecha'][:10]}</td>
    <td>{expediente}</td>
    <td><strong>{monto}</strong></td>
    <td>{ppto}</td>
    {_personas_resp_html}<td>{yo_resp}</td>
    <td>{seg['total_mensajes_hilo']}</td>
    <td>{cancha_fmt}</td>"""

            if incluir_ver:
                html += f"""
    <td><a class="ver-correo" href="{gmail_link}" target="_blank">Abrir</a></td>"""

            html += """
  </tr>
"""

    html += """</table>
"""

    # --- Tabla 2: Faltantes CC (solo si incluir_faltantes=True) ---
    if incluir_faltantes:
        # Generar titulo con nombres abreviados de personas clave
        _nombres_abrev = []
        for _key, _persona in PERSONAS_CLAVE.items():
            _partes = _persona["nombre"].split()
            _nombres_abrev.append(f"{_partes[0][0]}.{_partes[-1]}" if len(_partes) > 1 else _persona["nombre"])
        _titulo_faltantes = " / ".join(_nombres_abrev)

        html += f"""
<h3>2. CORREOS DONDE FALTAN {_titulo_faltantes.upper()} EN COPIA</h3>
<p style="font-size:11px; color:#666;">Nota: Si la persona es el remitente del correo, no se considera como faltante (ya tiene el correo).</p>
<table>
  <tr>
    <th>#</th>
    <th>Asunto</th>
    <th>De</th>"""
        for _abrev in _nombres_abrev:
            html += f"""
    <th>Falta {_abrev}</th>"""

        if incluir_ver:
            html += """
    <th>Ver</th>"""

        html += """
  </tr>
"""
        faltantes_count = 0
        for comp in comparativos:
            # Verificar faltantes dinamicamente por persona clave
            _faltantes_persona = {}
            _alguno_falta = False
            for _key, _persona in PERSONAS_CLAVE.items():
                _email_p = _persona.get("email", "")
                _en_copia = comp.get(f"{_key}_en_copia", False)
                _es_remitente = comp["de_email"].lower() == _email_p.lower() if _email_p else False
                _falta = not _en_copia and not _es_remitente
                _faltantes_persona[_key] = _falta
                if _falta:
                    _alguno_falta = True

            if _alguno_falta:
                faltantes_count += 1
                gmail_link = comp.get("gmail_link", "#")
                html += f"""  <tr>
    <td>{faltantes_count}</td>
    <td>{comp['asunto'][:55]}</td>
    <td>{comp['de_email']}</td>"""
                for _key in PERSONAS_CLAVE:
                    _falta = _faltantes_persona[_key]
                    html += f"""
    <td>{'<span class="rojo">FALTA</span>' if _falta else '<span class="verde">OK</span>'}</td>"""

                if incluir_ver:
                    html += f"""
    <td><a class="ver-correo" href="{gmail_link}" target="_blank">Abrir</a></td>"""

                html += """
  </tr>
"""

        if faltantes_count == 0:
            colspan = "6" if incluir_ver else "5"
            html += f'<tr><td colspan="{colspan}">Todos los comparativos tienen a ambas personas en copia.</td></tr>'

        html += """</table>
"""

    html += f"""
<div class="footer">
  Reporte generado automaticamente por Agente de Comparativos Gmail<br>
  Usuario: {mi_email} | Fecha: {datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}<br>
  <em>Nota: "Pdte. Rpta." = Pendiente de Respuesta por (quien debe actuar). CERRADO = la cadena ya fue atendida.</em>
</div>

</body>
</html>
"""
    return html


def _enviar_correo(service, de_email, destinatarios, asunto, html_body):
    """Envia un correo HTML a uno o mas destinatarios."""
    msg = MIMEText(html_body, "html")
    msg["to"] = ", ".join(destinatarios) if isinstance(destinatarios, list) else destinatarios
    msg["from"] = de_email
    msg["subject"] = asunto

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    sent = service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()

    print(f"[OK] Correo enviado a {msg['to']} (ID: {sent['id']})")
    return sent


def enviar_reporte(service, mi_email, comparativos):
    """Envia el reporte a todos los destinatarios con sus versiones personalizadas.

    Si MODO_PRUEBA esta activo, solo envia al usuario (mi_email).
    """
    asunto = f"[REPORTE] Estatus de Comparativos - {datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}"

    # 1. Usuario principal: reporte completo (con "Ver" y tabla faltantes) — SIEMPRE se envia
    html_usuario = generar_cuerpo_email(comparativos, mi_email, incluir_ver=True, incluir_faltantes=True)
    _enviar_correo(service, mi_email, mi_email, asunto, html_usuario)

    if MODO_PRUEBA:
        print("[MODO PRUEBA] Solo se envio al usuario. Otros destinatarios omitidos.")
        return

    # 2. Destinatarios con faltantes: sin "Ver", con tabla faltantes
    html_con_faltantes = generar_cuerpo_email(comparativos, mi_email, incluir_ver=False, incluir_faltantes=True)
    _enviar_correo(service, mi_email, DESTINATARIOS_CON_FALTANTES, asunto, html_con_faltantes)

    # 3. Destinatarios sin faltantes: sin "Ver", sin tabla faltantes
    html_sin_faltantes = generar_cuerpo_email(comparativos, mi_email, incluir_ver=False, incluir_faltantes=False)
    _enviar_correo(service, mi_email, DESTINATARIOS_SIN_FALTANTES, asunto, html_sin_faltantes)


def main():
    service = autenticar_gmail()
    mi_email = obtener_perfil(service)
    print(f"Conectado como: {mi_email}")

    with open(REPORT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    comparativos = data["comparativos"]
    print(f"Total correos en reporte: {len(comparativos)}")

    filtrados, excluidos = filtrar_comparativos(comparativos, mi_email)
    print(f"Comparativos reales: {len(filtrados)}")
    print(f"Excluidos (no son comparativos): {len(excluidos)}")

    if excluidos:
        print("\nCorreos excluidos:")
        for exc in excluidos:
            print(f"  - {exc['asunto']}")

    print(f"\nEnviando reportes...")
    enviar_reporte(service, mi_email, filtrados)
    print("Listo!")


if __name__ == "__main__":
    main()
