"""
ORQUESTADOR PRINCIPAL - Agente de Comparativos Gmail
====================================================
Ejecuta los agentes en secuencia:
  1. Busqueda y listado de comparativos
  2. Verificacion de CC
  3. Seguimiento de respuestas

Uso:
  python main.py                  # Ejecutar todo
  python main.py --solo-buscar    # Solo buscar y listar
  python main.py --solo-seguir    # Solo seguimiento
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# Zona horaria Peru (UTC-5)
PERU_TZ = timezone(timedelta(hours=-5))

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from config import REPORT_DIR, REPORT_FILE, REPORT_JSON, PERSONAS_CLAVE, MODO_PRUEBA, detectar_obra, USUARIO_NOMBRE
from auth_gmail import autenticar_gmail, autenticar_drive, autenticar_sheets, obtener_perfil
from agente_busqueda import buscar_comparativos
from agente_seguimiento import realizar_seguimiento
from drive_reader import extraer_datos_comparativo
from enviar_reporte import filtrar_comparativos

console = Console()


def main():
    parser = argparse.ArgumentParser(description="Agente de Comparativos - Gmail")
    parser.add_argument("--solo-buscar", action="store_true", help="Solo ejecutar busqueda")
    parser.add_argument("--solo-seguir", action="store_true", help="Solo ejecutar seguimiento")
    parser.add_argument("--max", type=int, default=100, help="Numero maximo de correos a buscar (default: 100)")
    args = parser.parse_args()

    console.print(Panel.fit(
        "[bold cyan]AGENTE DE COMPARATIVOS - GMAIL[/bold cyan]\n"
        f"Fecha: {datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}",
        border_style="cyan",
    ))

    # Autenticar
    console.print("\n[bold yellow]>>> AUTENTICACION[/bold yellow]")
    try:
        service = autenticar_gmail()
        mi_email = obtener_perfil(service)
        console.print(f"  Conectado como: [green]{mi_email}[/green]")
    except Exception as e:
        console.print(f"[bold red]Error de autenticacion: {e}[/bold red]")
        sys.exit(1)

    # === AGENTE 1: Busqueda ===
    console.print("\n[bold yellow]>>> AGENTE 1: BUSQUEDA DE COMPARATIVOS[/bold yellow]")
    comparativos = buscar_comparativos(service, max_results=args.max)

    if not comparativos:
        console.print("[bold red]No se encontraron correos de comparativos.[/bold red]")
        sys.exit(0)

    console.print(f"[bold]Correos encontrados: {len(comparativos)}[/bold]")

    # === FILTRAR correos que NO son comparativos reales (ANTES de Drive) ===
    console.print("\n[bold yellow]>>> FILTRANDO CORREOS NO RELEVANTES[/bold yellow]")
    comparativos_reales, excluidos = filtrar_comparativos(comparativos, mi_email)
    if excluidos:
        console.print(f"[dim]Excluidos (no son comparativos): {len(excluidos)}[/dim]")
        for exc in excluidos:
            console.print(f"  [dim]- {exc['asunto'][:60]}[/dim]")
    console.print(f"[bold]Comparativos reales para procesar: {len(comparativos_reales)}[/bold]")

    # === Extraer datos de archivos (Monto CC y PPTO META HG) solo para reales ===
    console.print("\n[bold yellow]>>> EXTRAYENDO DATOS DE ARCHIVOS ADJUNTOS Y DRIVE[/bold yellow]")
    try:
        drive_service = autenticar_drive()
        sheets_service = autenticar_sheets()

        for i, comp in enumerate(comparativos_reales):
            console.print(f"  [{i+1}/{len(comparativos_reales)}] {comp['asunto'][:50]}...", end=" ")
            try:
                datos = extraer_datos_comparativo(
                    service, drive_service, sheets_service,
                    comp["id"], comp.get("cuerpo_preview", ""),
                    asunto=comp.get("asunto", ""),
                    thread_id=comp.get("thread_id", "")
                )
                if datos:
                    if datos.get("monto_cc") != "No especificado":
                        comp["monto"] = datos["monto_cc"]
                    if datos.get("ppto_meta_hg") != "No especificado":
                        comp["ppto_meta_hg"] = datos["ppto_meta_hg"]
                    if datos.get("expediente") != "No especificado":
                        comp["expediente"] = datos["expediente"]
                console.print(f"[green]OK[/green] (Monto: {comp['monto']}, PPTO: {comp.get('ppto_meta_hg', 'N/A')}, EXP: {comp.get('expediente', 'N/A')})")
            except Exception as e:
                console.print(f"[yellow]SKIP[/yellow] ({e})")
    except Exception as e:
        console.print(f"[yellow]Drive/Sheets no disponible: {e}. Usando datos del correo.[/yellow]")

    _mostrar_tabla_comparativos(comparativos_reales)

    if args.solo_buscar:
        _guardar_reporte(comparativos_reales, [], mi_email)
        console.print("\n[green]Reporte guardado. Ejecuta sin --solo-buscar para ver mas.[/green]")
        return

    # === AGENTE 2: Seguimiento (solo comparativos reales) ===
    console.print("\n[bold yellow]>>> AGENTE 3: SEGUIMIENTO DE RESPUESTAS[/bold yellow]")
    seguimiento = realizar_seguimiento(service, comparativos_reales, mi_email)
    _mostrar_tabla_seguimiento(seguimiento)

    # Guardar reporte
    _guardar_reporte(comparativos_reales, seguimiento, mi_email)

    console.print(Panel.fit(
        "[bold green]PROCESO COMPLETADO[/bold green]\n"
        f"Comparativos encontrados: {len(comparativos)}\n"
        f"Reporte guardado en: {REPORT_DIR}",
        border_style="green",
    ))


def _mostrar_tabla_comparativos(comparativos):
    """Muestra tabla resumen de comparativos encontrados."""
    table = Table(title="COMPARATIVOS ENCONTRADOS", show_lines=True)
    table.add_column("#", style="cyan", width=4)
    table.add_column("Fecha", style="white", width=12)
    table.add_column("Asunto", style="bold white", max_width=40)
    table.add_column("De", style="yellow", max_width=25)
    table.add_column("Monto", style="green", width=15)
    # Columnas dinamicas por persona clave
    for _key, _persona in PERSONAS_CLAVE.items():
        _partes = _persona["nombre"].split()
        _abrev = f"{_partes[0][0]}.{_partes[-1]}" if len(_partes) > 1 else _persona["nombre"]
        table.add_column(_abrev, style="white", width=10)
    table.add_column("Resumen", max_width=40)

    for i, comp in enumerate(comparativos, 1):
        # Generar status por persona clave
        _personas_status = []
        for _key in PERSONAS_CLAVE:
            _info = comp["personas_en_copia"].get(_key, {})
            _personas_status.append("[green]SI[/green]" if _info.get("en_copia") else "[red]NO[/red]")

        table.add_row(
            str(i),
            comp["fecha"][:10] if comp["fecha"] else "-",
            comp["asunto"][:40],
            comp["de_email"][:25] if comp.get("de_email") else comp["de"][:25],
            comp["monto"],
            *_personas_status,
            comp["resumen"][:40] + "..." if len(comp["resumen"]) > 40 else comp["resumen"],
        )

    console.print(table)


def _mostrar_tabla_seguimiento(seguimiento):
    """Muestra tabla de seguimiento de respuestas."""
    table = Table(title="SEGUIMIENTO DE RESPUESTAS", show_lines=True)
    table.add_column("#", style="cyan", width=4)
    table.add_column("Asunto", style="bold white", max_width=35)
    table.add_column("Monto", style="green", width=15)
    # Columnas dinamicas por persona clave
    for _key, _persona in PERSONAS_CLAVE.items():
        _partes = _persona["nombre"].split()
        _abrev = f"{_partes[0][0]}.{_partes[-1]}" if len(_partes) > 1 else _persona["nombre"]
        table.add_column(_abrev, style="white", width=12)
    table.add_column(USUARIO_NOMBRE, style="white", width=12)
    table.add_column("Msgs", style="cyan", width=5)
    table.add_column("Pdte. Rpta.", style="white", width=18)

    for i, seg in enumerate(seguimiento, 1):
        en_cancha = seg.get("en_cancha_de", "PENDIENTE")
        if en_cancha == "CERRADO":
            cancha_fmt = "[green]CERRADO[/green]"
        else:
            cancha_fmt = f"[red]{en_cancha}[/red]"

        # Generar columnas de respuesta dinamicamente
        _personas_fmt = []
        for _key in PERSONAS_CLAVE:
            _resp = seg["respuestas"].get(_key, {})
            _personas_fmt.append("[green]Respondio[/green]" if _resp.get("respondio") else "[red]Pendiente[/red]")
        yo = seg["respuestas"].get("yo", {})
        yo_fmt = "[green]Respondio[/green]" if yo.get("respondio") else "[red]Pendiente[/red]"

        table.add_row(
            str(i),
            seg["asunto"][:35],
            seg.get("monto", "-"),
            *_personas_fmt,
            yo_fmt,
            str(seg["total_mensajes"]),
            cancha_fmt,
        )

    console.print(table)


def _guardar_reporte(comparativos, seguimiento, mi_email):
    """Guarda los resultados en archivos de reporte."""
    os.makedirs(REPORT_DIR, exist_ok=True)

    # Reporte JSON
    data = {
        "fecha_ejecucion": datetime.now(PERU_TZ).isoformat(),
        "usuario": mi_email,
        "total_comparativos": len(comparativos),
        "comparativos": [],
    }

    for comp in comparativos:
        seg_item = next((s for s in seguimiento if s["id"] == comp["id"]), {}) if seguimiento else {}

        data["comparativos"].append({
            "id": comp["id"],
            "asunto": comp["asunto"],
            "de": comp["de"],
            "de_email": comp.get("de_email", ""),
            "fecha": comp["fecha"],
            "monto": comp["monto"],
            "ppto_meta_hg": comp.get("ppto_meta_hg", "No especificado"),
            "expediente": comp.get("expediente", "No especificado"),
            "obra": detectar_obra(comp["asunto"], comp.get("de_email", "")),
            "resumen": comp["resumen"],
            "gmail_link": comp.get("gmail_link", ""),
            **{f"{_key}_en_copia": comp["personas_en_copia"].get(_key, {}).get("en_copia", False) for _key in PERSONAS_CLAVE},
            "seguimiento": {
                "estado": seg_item.get("estado_general", "N/A"),
                "en_cancha_de": seg_item.get("en_cancha_de", "PENDIENTE"),
                **{f"{_key}_respondio": seg_item.get("respuestas", {}).get(_key, {}).get("respondio", False) for _key in PERSONAS_CLAVE},
                "yo_respondi": seg_item.get("respuestas", {}).get("yo", {}).get("respondio", False),
                "total_mensajes_hilo": seg_item.get("total_mensajes", 0),
                "cadena_completa": seg_item.get("cadena_completa", False),
            },
        })

    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Reporte texto
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("REPORTE DE COMPARATIVOS - GMAIL\n")
        f.write(f"Fecha: {datetime.now(PERU_TZ).strftime('%d/%m/%Y %H:%M')}\n")
        f.write(f"Usuario: {mi_email}\n")
        f.write("=" * 70 + "\n\n")

        for i, comp in enumerate(data["comparativos"], 1):
            f.write(f"--- Comparativo #{i} ---\n")
            f.write(f"  Asunto:  {comp['asunto']}\n")
            f.write(f"  De:      {comp['de']}\n")
            f.write(f"  Fecha:   {comp['fecha']}\n")
            f.write(f"  Monto:   {comp['monto']}\n")
            f.write(f"  Resumen: {comp['resumen']}\n")
            for _key, _persona in PERSONAS_CLAVE.items():
                _en_cc = comp.get(f"{_key}_en_copia", False)
                f.write(f"  {_persona['nombre']} en CC: {'SI' if _en_cc else 'NO'}\n")
            seg = comp["seguimiento"]
            f.write(f"  Estado seguimiento:   {seg['estado']}\n")
            for _key, _persona in PERSONAS_CLAVE.items():
                _respondio = seg.get(f"{_key}_respondio", False)
                f.write(f"    - {_persona['nombre']} respondio: {'SI' if _respondio else 'NO'}\n")
            f.write(f"    - {USUARIO_NOMBRE} respondio: {'SI' if seg['yo_respondi'] else 'NO'}\n")
            f.write(f"    - Total mensajes en hilo:   {seg['total_mensajes_hilo']}\n")
            f.write("\n")

    console.print(f"\n[dim]Reportes guardados en:[/dim]")
    console.print(f"  [dim]JSON: {REPORT_JSON}[/dim]")
    console.print(f"  [dim]TXT:  {REPORT_FILE}[/dim]")


if __name__ == "__main__":
    main()
