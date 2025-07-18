/*************  ✨ Windsurf Command 🌟  *************/
#!/usr/bin/env python3
"""
Script para verificar si la API Key de Google funciona correctamente.
"""

import os
import requests
from dotenv import load_dotenv
from rich.console import Console

console = Console()
rprint = console.print

def test_api_key():
    """Prueba la API Key con una petición simple."""
    
    # Cargar variables de entorno
    load_dotenv()
    api_key = os.getenv("GOOGLE_API_KEY")
    
    # Verificaciones básicas
    rprint("[bold blue]🔍 Verificando API Key...[/bold blue]")
    
    if not api_key:
        rprint("[red]❌ ERROR: No se encontró GOOGLE_API_KEY en .env[/red]")
        rprint("[yellow]💡 Crea un archivo .env con: GOOGLE_API_KEY=tu_api_key[/yellow]")
        return False
    
    rprint(f"[green]✅ API Key encontrada:[/green] {api_key[:20]}...")
    
    # Probar con una petición simple de geocoding
    test_url = "https://maps.googleapis.com/maps/api/geocode/json"
    test_params = {
        "address": "Madrid, España",
        "key": api_key
    }
    
    try:
        rprint("[blue]🌐 Probando conexión con Google API...[/blue]")
        response = requests.get(test_url, params=test_params)
        data = response.json()
        
        status = data.get("status")
        rprint(f"[cyan]📡 Status de respuesta:[/cyan] {status}")
        
        if status == "OK":
            rprint("[green]🎉 ¡API Key funciona correctamente![/green]")
            result = data["results"][0]
            location = result["geometry"]["location"]
            rprint(f"[dim]Coordenadas de prueba: {location['lat']}, {location['lng']}[/dim]")
            return True
            
        elif status == "REQUEST_DENIED":
            rprint("[red]❌ REQUEST_DENIED - Problemas con la API Key[/red]")
            
            error_message = data.get("error_message", "")
            rprint(f"[red]Mensaje de error:[/red] {error_message}")
            
            rprint("\n[yellow]🔧 Posibles soluciones:[/yellow]")
            rprint("1. Verificar que la API Key es correcta")
            rprint("2. Habilitar Geocoding API en Google Cloud Console")
            rprint("3. Verificar restricciones de la API Key")
            rprint("4. Verificar facturación en Google Cloud")
            
            return False
            
        elif status == "OVER_QUERY_LIMIT":
            rprint("[yellow]⚠️ Límite de consultas excedido[/yellow]")
            return False
            
        else:
            rprint(f"[red]❌ Error desconocido:[/red] {status}")
            return False
            
    except Exception as e:
        rprint(f"[red]❌ Error de conexión:[/red] {str(e)}")
        return False

def show_setup_instructions():
    """Muestra instrucciones para configurar la API Key."""
    
    rprint("\n[bold yellow]📋 Instrucciones para obtener API Key:[/bold yellow]")
    rprint("1. Ve a: https://console.cloud.google.com/")
    rprint("2. Crea un proyecto nuevo o selecciona uno existente")
    rprint("3. Habilita las APIs:")
    rprint("   - Geocoding API")
    rprint("   - Places API")
    rprint("4. Ve a 'Credenciales' > 'Crear credenciales' > 'Clave de API'")
    rprint("5. Copia la API Key y ponla en el archivo .env")
    rprint("\n[bold cyan]📄 Formato del archivo .env:[/bold cyan]")
    rprint("GOOGLE_API_KEY=AIzaSyBxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

if __name__ == "__main__":
    rprint("[bold green]🧪 Test de API Key de Google[/bold green]\n")
    
    success = test_api_key()
    
    if not success:
        show_setup_instructions()
    
    rprint(f"\n[bold]Resultado: {'✅ ÉXITO' if success else '❌ FALLO'}[/bold]")
/*******  20965e9e-a1db-4dd4-a7ea-d9038ffc8cb3  *******/