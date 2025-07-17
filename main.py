import json
import math
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path

import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table


console = Console()
rprint = console.print


@dataclass
class Coordinates:
    lat: float
    lng: float
    formatted_address: str
    precision: str


@dataclass
class BusinessDetails:
    nombre: str
    direccion: Optional[str]
    link_google_maps: str
    valoracion: Optional[float]
    categorias: str
    telefono: Optional[str]
    sitio_web: Optional[str]
    referencia_poligono: str
    coordenadas_poligono: str
    precision_ubicacion: str


@dataclass
class ScanResult:
    poligono: str
    negocios_encontrados: int
    estado: str
    timestamp: str
    error: Optional[str] = None


class GoogleAPIError(Exception):
    pass


class TypeTranslator:

    TRANSLATIONS: Dict[str, str] = {
        "restaurant": "restaurante",
        "lodging": "alojamiento",
        "point_of_interest": "punto de interés",
        "establishment": "establecimiento",
        "store": "tienda",
        "school": "escuela",
        "gas_station": "gasolinera",
        "pharmacy": "farmacia",
        "gym": "gimnasio",
        "car_repair": "taller de coches",
        "electronics_store": "tienda de electrónica",
        "supermarket": "supermercado",
        "bakery": "panadería",
        "bank": "banco",
        "locality": "localidad",
        "political": "zona administrativa"
    }
    
    @classmethod
    def translate_types(cls, types: List[str]) -> str:
        return ", ".join([cls.TRANSLATIONS.get(t, t) for t in types])


class GoogleGeocodingService:
    
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    

    def _generate_search_variations(self, polygon_name: str) -> List[str]:
        return [
            f"{polygon_name}, Madrid, España",
            f"Polígono Industrial {polygon_name}, Madrid",
            f"{polygon_name}, Madrid",
            f"Polígono {polygon_name}, Madrid, España"
        ]
    

    def get_coordinates(
        self, 
        polygon_name: str, 
        max_retries: int = 3
    ) -> Optional[Coordinates]:
        """
        Obtiene coordenadas para un polígono usando Geocoding API
        
        Args:
            polygon_name: Nombre del polígono a buscar
            max_retries: Número máximo de reintentos
            
        Returns:
            Coordinates object o None si no se encuentran
        """
        rprint(f"[bold blue]Buscando para:[/bold blue] {polygon_name}")
        
        search_variations = self._generate_search_variations(polygon_name)
        
        for variation in search_variations:
            for attempt in range(max_retries):
                try:
                    params = {
                        "address": variation,
                        "key": self.api_key,
                        "region": "es",
                        "components": "country:ES|administrative_area:Madrid"
                    }
                    
                    response = requests.get(self.base_url, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get("status") == "OK" and data.get("results"):
                        result = data["results"][0]
                        location = result["geometry"]["location"]
                        
                        coordinates = Coordinates(
                            lat=location["lat"],
                            lng=location["lng"],
                            formatted_address=result["formatted_address"],
                            precision=result["geometry"]["location_type"]
                        )
                        
                        rprint(
                            f"[green]Coordenadas encontradas:[/green] "
                            f"lat={coordinates.lat}, lng={coordinates.lng}"
                        )
                        rprint(f"[dim]Dirección: {coordinates.formatted_address}[/dim]")
                        
                        return coordinates
                    
                    elif data.get("status") == "ZERO_RESULTS":
                        rprint(f"[yellow]Sin resultados para:[/yellow] {variation}")
                        continue
                        
                    elif data.get("status") == "OVER_QUERY_LIMIT":
                        rprint("[red]Límite de consultas excedido. Esperando...[/red]")
                        time.sleep(5)
                        continue
                        
                    else:
                        rprint(f"[red]Error en geocoding:[/red] {data.get('status')}")
                        
                except requests.RequestException as e:
                    rprint(f"[red]Error en intento {attempt + 1}:[/red] {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
        
        rprint(f"[red]No se pudieron obtener coordenadas para:[/red] {polygon_name}")
        return None


class GooglePlacesService:
    
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.places_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        self.details_url = "https://maps.googleapis.com/maps/api/place/details/json"
    

    def get_place_details(self, place_id: str) -> Dict[str, Optional[str]]:
        params = {
            "place_id": place_id,
            "fields": "formatted_phone_number,website",
            "key": self.api_key
        }
        
        try:
            response = requests.get(self.details_url, params=params)
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") == "OK":
                return result.get("result", {})
        except requests.RequestException:
            pass
        
        return {}
    

    def search_nearby_businesses(
        self, 
        coordinates: Coordinates, 
        radius: int = 320
    ) -> List[Dict]:
        """
        Busca negocios en un radio alrededor de las coordenadas
        
        Args:
            coordinates: Coordenadas del centro de búsqueda
            radius: Radio de búsqueda en metros
            
        Returns:
            Lista de negocios encontrados
        """
        all_businesses = {}
        
        params = {
            "location": f"{coordinates.lat},{coordinates.lng}",
            "radius": radius,
            "key": self.api_key
        }
        
        while True:
            try:
                response = requests.get(self.places_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") != "OK":
                    rprint(f"[red]Error en búsqueda:[/red] {data.get('status')}")
                    break
                
                for place in data.get("results", []):
                    place_id = place.get("place_id")
                    if place_id not in all_businesses:
                        all_businesses[place_id] = place
                
                if "next_page_token" in data:
                    params["pagetoken"] = data["next_page_token"]
                    rprint("[dim]Cargando más resultados...[/dim]")
                    time.sleep(3)
                else:
                    break
                    
            except requests.RequestException as e:
                rprint(f"[red]Error en búsqueda de lugares:[/red] {str(e)}")
                break
        
        return list(all_businesses.values())


class PolygonBusinessScraper:

    def __init__(self, api_key: str) -> None:
        
        self.api_key = api_key
        self.geocoding_service = GoogleGeocodingService(api_key)
        self.places_service = GooglePlacesService(api_key)
        self.type_translator = TypeTranslator()
    

    def _create_business_details(
        self, 
        place: Dict, 
        coordinates: Coordinates, 
        polygon_name: str
    ) -> BusinessDetails:
        
        place_id = place.get("place_id")
        details = self.places_service.get_place_details(place_id)
        
        return BusinessDetails(
            nombre=place.get("name", ""),
            direccion=place.get("vicinity"),
            link_google_maps=f"https://www.google.com/maps/place/?q=place_id:{place_id}",
            valoracion=place.get("rating"),
            categorias=self.type_translator.translate_types(place.get("types", [])),
            telefono=details.get("formatted_phone_number"),
            sitio_web=details.get("website"),
            referencia_poligono=polygon_name,
            coordenadas_poligono=f"{coordinates.lat},{coordinates.lng}",
            precision_ubicacion=coordinates.precision
        )
    

    def scan_polygon(
        self, 
        polygon_name: str, 
        search_radius: int = 320
    ) -> Optional[int]:
        """
        Escanea un polígono y guarda los negocios encontrados
        
        Args:
            polygon_name: Nombre del polígono a escanear
            search_radius: Radio de búsqueda en metros
            
        Returns:
            Número de negocios encontrados o None si hay error
        """
        rprint(f"\n[bold green]Iniciando escaneo:[/bold green] {polygon_name}")
        
        coordinates = self.geocoding_service.get_coordinates(polygon_name)
        if not coordinates:
            rprint(
                f"[red]No se puede escanear {polygon_name} - "
                f"coordenadas no encontradas[/red]"
            )
            return None
        
        rprint(
            f"[blue]Buscando negocios en radio de {search_radius}m "
            f"alrededor de: {coordinates.lat}, {coordinates.lng}[/blue]"
        )
        
        places = self.places_service.search_nearby_businesses(
            coordinates, search_radius
        )

        businesses = []
        for place in places:
            business = self._create_business_details(place, coordinates, polygon_name)
            businesses.append(business.__dict__)
        
        filename = self._generate_filename(polygon_name)
        self._save_businesses_to_file(businesses, filename)
        
        rprint(
            f"[green]✓ {len(businesses)} negocios guardados en ./data/{filename}[/green]"
        )
        return len(businesses)
    
    def _generate_filename(self, polygon_name: str) -> str:

        safe_name = (polygon_name.lower()
                    .replace(' ', '_')
                    .replace('ñ', 'n')
                    .replace('á', 'a')
                    .replace('é', 'e')
                    .replace('í', 'i')
                    .replace('ó', 'o')
                    .replace('ú', 'u'))
        return f"negocios_{safe_name}.json"
    

    def _save_businesses_to_file(
        self, 
        businesses: List[Dict], 
        filename: str
    ) -> None:

        os.makedirs("./data", exist_ok=True)
        filepath = f"./data/{filename}"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(businesses, f, ensure_ascii=False, indent=2)


class BatchProcessor:
    
    def __init__(self, scraper: PolygonBusinessScraper) -> None:
        self.scraper = scraper
    

    def process_polygon_list(
        self, 
        polygon_names: List[str], 
        output_file: str = "./data/resumen_escaneo.json"
    ) -> List[ScanResult]:
        """
        Procesa una lista completa de polígonos
        
        Args:
            polygon_names: Lista de nombres de polígonos
            output_file: Archivo para guardar resumen
            
        Returns:
            Lista de resultados de escaneo
        """
        rprint(
            f"[bold blue]Iniciando procesamiento de {len(polygon_names)} "
            f"polígonos...[/bold blue]"
        )
        
        results = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            task = progress.add_task(
                "Procesando polígonos...", 
                total=len(polygon_names)
            )
            
            for i, polygon_name in enumerate(polygon_names, 1):
                progress.update(
                    task, 
                    description=f"Procesando {i}/{len(polygon_names)}: {polygon_name}"
                )
                
                try:
                    business_count = self.scraper.scan_polygon(polygon_name)
                    result = ScanResult(
                        poligono=polygon_name,
                        negocios_encontrados=business_count or 0,
                        estado="completado" if business_count is not None else "fallido",
                        timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
                    )
                except Exception as e:
                    rprint(f"[red]Error procesando {polygon_name}: {str(e)}[/red]")
                    result = ScanResult(
                        poligono=polygon_name,
                        negocios_encontrados=0,
                        estado="error",
                        timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
                        error=str(e)
                    )
                
                results.append(result)
                
                if i % 10 == 0:
                    self._save_summary(results, output_file)
                    rprint(f"[dim]Progreso guardado: {i}/{len(polygon_names)}[/dim]")

                time.sleep(1)
                progress.advance(task)
        

        self._save_summary(results, output_file)
        self._display_summary_table(results)
        
        rprint(
            f"\n[bold green]Procesamiento completado. "
            f"Resumen guardado en {output_file}[/bold green]"
        )
        return results
    

    def _save_summary(self, results: List[ScanResult], filename: str) -> None:
        """Guarda resumen de resultados."""
        os.makedirs("./data", exist_ok=True)
        results_dict = [result.__dict__ for result in results]
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results_dict, f, ensure_ascii=False, indent=2)
    
    
    def _display_summary_table(self, results: List[ScanResult]) -> None:
        """Muestra tabla resumen de resultados."""
        table = Table(title="Resumen de Escaneo")
        table.add_column("Estado", style="cyan")
        table.add_column("Cantidad", justify="right", style="magenta")
        
        status_counts = {}
        total_businesses = 0
        
        for result in results:
            status_counts[result.estado] = status_counts.get(result.estado, 0) + 1
            total_businesses += result.negocios_encontrados
        
        for status, count in status_counts.items():
            table.add_row(status.title(), str(count))
        
        table.add_row("", "")
        table.add_row("Total Negocios", str(total_businesses))
        
        console.print(table)


class ScraperApp:
    
    def __init__(self) -> None:
        self.api_key = self._load_api_key()
        self.scraper = PolygonBusinessScraper(self.api_key)
        self.batch_processor = BatchProcessor(self.scraper)
    
    
    def _load_api_key(self) -> str:

        load_dotenv()
        api_key = os.getenv("GOOGLE_API_KEY")
        
        if not api_key:
            rprint("[red]ERROR: No se encontró GOOGLE_API_KEY en .env[/red]")
            raise GoogleAPIError("API Key no encontrada")
        
        return api_key
    

    def run_test_mode(self) -> None:

        test_polygons = [
            "Los Olivos",
            "Nuestra Señora de Butarque",
            "Las Rozas"
        ]
        
        rprint("[bold yellow]Modo prueba activado[/bold yellow]")
        self.batch_processor.process_polygon_list(test_polygons)
    

    def scan_single_polygon(self, polygon_name: str) -> None:
        self.scraper.scan_polygon(polygon_name)
    

    def process_polygon_file(self, file_path: str) -> None:

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            polygon_names = data["names"]
            
            self.batch_processor.process_polygon_list(polygon_names)
            
        except Exception as e:
            rprint(f"[red]Error leyendo archivo: {e}[/red]")
    

    def show_usage(self) -> None:

        rprint("\n[bold blue]Uso:[/bold blue]")
        rprint("  python scraper.py --test                    # Modo prueba")
        rprint("  python scraper.py --poligon 'Los Olivos'    # Polígono específico")
        rprint("  python scraper.py --file poligonos.json     # Procesar lista completa")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Scraper de polígonos industriales"
    )
    parser.add_argument("--poligon", type=str, help="Nombre del polígono específico")
    parser.add_argument(
        "--file", 
        type=str, 
        help="Archivo JSON con lista de nombres de polígonos"
    )
    parser.add_argument("--test", action="store_true", help="Modo prueba")
    
    args = parser.parse_args()
    
    try:
        app = ScraperApp()
        
        if args.test:
            app.run_test_mode()
        elif args.poligon:
            app.scan_single_polygon(args.poligon)
        elif args.file:
            app.process_polygon_file(args.file)
        else:
            app.show_usage()
            
    except GoogleAPIError as e:
        rprint(f"[red]Error de API: {e}[/red]")
    except Exception as e:
        rprint(f"[red]Error inesperado: {e}[/red]")


if __name__ == "__main__":
    main()