import json
import requests
import re
import time
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from rich.console import Console

console = Console()


class EmailExtractor:
    def __init__(self):
        self.root = Path(__file__).parent
        self.data_dir = self.root / '../data'
        self.output_dir = self.data_dir / '../output'
        self.contact_paths = ["/contacto", "/contact", "/empresa", "/about", "/quienes-somos"]
        self.data_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)


    def extract_email_from_web(self, url):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                return None
            
            soup = BeautifulSoup(res.text, "html.parser")
            emails = set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", soup.text))
            
            filtered_emails = [email for email in emails if not any(
                spam in email.lower() for spam in ['noreply', 'no-reply', 'admin@', 'webmaster@']
            )]
            
            return filtered_emails[0] if filtered_emails else None
        except Exception:
            return None


    def get_email(self, website):
        if not website or "google.com" in website or "gmb" in website:
            return None
        
        email = self.extract_email_from_web(website)
        if email:
            return email
        
        for path in self.contact_paths:
            if website.endswith("/"):
                url = website[:-1] + path
            else:
                url = website + path
            
            email = self.extract_email_from_web(url)
            if email:
                return email
        
        return None


    def validate_email_format(self, email):
        if not email:
            return False
        return re.match(r"[^@\s]+@[^@\s]+\.[a-zA-Z0-9]+$", email) is not None


    def process_json_file(self, json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                businesses = json.load(f)
            
            if not isinstance(businesses, list):
                return False
            
            emails_found = 0
            
            for i, business in enumerate(businesses, 1):
                website = business.get("sitio_web")
                
                if website:
                    console.print(f"[cyan][i][/cyan] Procesando [{i}/{len(businesses)}]: {website}")
                    email = self.get_email(website)
                    
                    if email and self.validate_email_format(email):
                        business["email"] = email
                        emails_found += 1
                        console.print(f"[green][✓][/green] Email encontrado: {email}")
                    else:
                        business["email"] = None
                        console.print(f"[yellow][!][/yellow] No se encontró email")
                    
                    time.sleep(2)
                else:
                    business["email"] = None
            
            output_file = self.output_dir / json_file.name
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(businesses, f, ensure_ascii=False, indent=2)
            
            console.print(f"[green][✓][/green] Emails encontrados: {emails_found}/{len(businesses)}")
            console.print(f"[blue][i][/blue] Guardado en: {output_file.name}")
            return True
            
        except Exception as e:
            console.print(f"[red][!][/red] Error procesando {json_file.name}: {e}")
            return False


    def execute(self):
        console.print("[bold blue][i] Iniciando proceso de extracción de emails[/bold blue]")
        console.print("=" * 50)
        
        if not self.data_dir.exists():
            console.print(f"[red][!][/red] El directorio '{self.data_dir}' no existe.")
            return
        
        json_files = list(self.data_dir.glob('negocios_*.json'))
        
        if not json_files:
            console.print("[red][!][/red] No se encontraron archivos JSON con patrón 'negocios_*.json'")
            return
        
        console.print(f"[green][✓][/green] Encontrados {len(json_files)} archivos JSON:")
        for file in json_files:
            console.print(f"  [cyan]•[/cyan] {file.name}")
        
        successful_files = 0
        
        for json_file in json_files:
            console.print(f"\n[bold cyan][i] Procesando: {json_file.name}[/bold cyan]")
            if self.process_json_file(json_file):
                successful_files += 1
        
        console.print(f"\n[bold green][✓] ¡Proceso completado exitosamente![/bold green]")
        console.print(f"[blue][i][/blue] Archivos procesados: {successful_files}/{len(json_files)}")


def main():
    extractor = EmailExtractor()
    extractor.execute()


if __name__ == '__main__':
    main()