import json
import re
import asyncio
import os
import glob
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from typing import Dict, List, Optional, Tuple
import logging
import time
from datetime import datetime
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConcurrentContactVerifier:
    def __init__(self, max_concurrent_workers: int = 3, max_browsers: int = 2):
        self.max_concurrent_workers = max_concurrent_workers
        self.max_browsers = max_browsers
        
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.phone_pattern = re.compile(r'(\+34|0034)?\s*[6-9]\d{8}|(\+34|0034)?\s*[89]\d{8}')
        
        self.invalid_email_extensions = {
            '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico', '.bmp',
            '.css', '.js', '.html', '.htm', '.pdf', '.doc', '.docx', '.xlsx',
            '.zip', '.rar', '.mp4', '.mp3', '.avi', '.mov', '.woff', '.ttf',
            '.json', '.xml', '.txt', '.log', '.tmp', '.cache', '.tiff', '.tif',
            '.eps', '.psd', '.ai', '.sketch', '.fig', '.xd', '.indd', '.raw',
            '.cr2', '.nef', '.arw', '.dng', '.orf', '.rw2', '.pef', '.sr2',
            '.3gp', '.flv', '.mkv', '.wmv', '.webm', '.m4v', '.m4a', '.aac',
            '.flac', '.ogg', '.wav', '.wma', '.opus', '.mid', '.midi', '.kar',
            '.woff2', '.eot', '.otf', '.swf', '.fla', '.as', '.scss', '.sass',
            '.less', '.styl', '.coffee', '.ts', '.jsx', '.tsx', '.vue', '.svelte',
            '.php', '.asp', '.aspx', '.jsp', '.cfm', '.cgi', '.pl', '.py', '.rb',
            '.go', '.rs', '.kt', '.swift', '.dart', '.scala', '.clj', '.hs',
            '.elm', '.purs', '.ml', '.fs', '.vb', '.cs', '.java', '.c', '.cpp',
            '.h', '.hpp', '.cc', '.cxx', '.m', '.mm', '.s', '.asm', '.sql',
            '.db', '.sqlite', '.mdb', '.accdb', '.dbf', '.backup', '.bak',
            '.old', '.orig', '.save', '.temp', '.lock', '.pid', '.sock',
            '.err', '.out', '.trace', '.dump', '.core', '.crash', '.dmp'
        }
        
        self.technical_domains = {
            'sentry.io', 'ingest.sentry.io', 'sentry.wixpress.com', 'sentry.com',
            'googletagmanager.com', 'google-analytics.com', 'facebook.com',
            'doubleclick.net', 'googlesyndication.com', 'googleadservices.com',
            'cloudflare.com', 'amazonaws.com', 'googleapis.com', 'gstatic.com',
            'jsdelivr.net', 'unpkg.com', 'cdnjs.cloudflare.com',
            'tracking.com', 'analytics.com', 'metrics.com', 'cdn.com',
            'static.com', 'assets.com', 'media.com', 'img.com', 'images.com',
            'fonts.com', 'typekit.com', 'adobe.com', 'gravatar.com',
            'disqus.com', 'addthis.com', 'sharethis.com', 'feedburner.com',
            'feedproxy.google.com', 'mailchimp.com', 'constantcontact.com',
            'aweber.com', 'getresponse.com', 'campaignmonitor.com',
            'verticalresponse.com', 'icontact.com', 'madmimi.com',
            'benchmark.email', 'sendinblue.com', 'mailgun.com', 'sendgrid.com',
            'mandrill.com', 'postmark.com', 'sparkpost.com', 'ses.amazonaws.com'
        }
        
        self.technical_id_patterns = [
            re.compile(r'^[a-f0-9]{32}@'),
            re.compile(r'^[a-f0-9]{40}@'),
            re.compile(r'^[a-f0-9]{64}@'),
            re.compile(r'^[a-z0-9]{20,}@'),
        ]
        
        self.contact_paths = [
            '',
            '/contacto',
            '/contact',
            '/contacta',
            '/sobre-nosotros',
            '/about',
            '/info',
            '/informacion'
        ]
        
        self.semaphore = asyncio.Semaphore(max_concurrent_workers)
        self.browser_semaphore = asyncio.Semaphore(max_browsers)
        
        self.progress_lock = threading.Lock()
        self.processed_count = 0
        self.verified_emails_count = 0
        self.verified_phones_count = 0
        self.results = []
        
        self.browsers = []
        self.browser_queue = asyncio.Queue()
        self.playwright = None

    async def initialize_browsers(self):
        logger.info(f"Inicializando {self.max_browsers} navegadores...")
        
        self.playwright = await async_playwright().start()
        
        for i in range(self.max_browsers):
            browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--memory-pressure-off',
                    '--max_old_space_size=512'
                ]
            )
            self.browsers.append(browser)
            await self.browser_queue.put(browser)

    async def get_browser(self) -> Browser:
        async with self.browser_semaphore:
            return await self.browser_queue.get()

    async def return_browser(self, browser: Browser):
        await self.browser_queue.put(browser)

    async def cleanup_browsers(self):
        logger.info("Cerrando navegadores...")
        for browser in self.browsers:
            try:
                await browser.close()
            except:
                pass
        self.browsers.clear()
        
        try:
            await self.playwright.stop()
        except:
            pass

    def is_valid_email(self, email: str) -> bool:
        if not email:
            return False
        
        email = email.lower().strip()
        
        if len(email) < 5 or len(email) > 100:
            return False
        
        for ext in self.invalid_email_extensions:
            if email.endswith(ext):
                return False
        
        if email.count('@') != 1:
            return False
        
        local, domain = email.split('@')
        
        for tech_domain in self.technical_domains:
            if domain == tech_domain or domain.endswith('.' + tech_domain):
                return False
        
        for pattern in self.technical_id_patterns:
            if pattern.match(email):
                return False
        
        if len(local) < 1 or len(local) > 64:
            return False
        
        if local.startswith('.') or local.endswith('.'):
            return False
        
        if '..' in local:
            return False
        
        if len(local) > 20 and re.match(r'^[a-f0-9]+$', local):
            return False
        
        if len(domain) < 3 or len(domain) > 253:
            return False
        
        if '.' not in domain:
            return False
        
        if domain.startswith('.') or domain.endswith('.') or domain.startswith('-') or domain.endswith('-'):
            return False
        
        parts = domain.split('.')
        if len(parts[-1]) < 2:
            return False
        
        problematic_patterns = [
            'ajax-loader', 'spinner', 'loading', 'loader',
            'kit_mobile', 'mobile_kit', 'responsive',
            '@2x', '@3x', 'retina', 'thumbnail', 'logo',
            'ingest', 'tracking', 'analytics', 'metrics',
            'sentry', 'error', 'crash', 'debug',
            'api_key', 'access_token', 'session_id',
            'image', 'img', 'picture', 'photo', 'icon',
            'asset', 'resource', 'static', 'public',
            'example', 'test', 'sample', 'placeholder', 'dummy',
            'fake', 'invalid', 'noreply', 'donotreply'
        ]
        
        for pattern in problematic_patterns:
            if pattern in email:
                return False
        
        domain_parts = domain.split('.')
        if len(domain_parts) > 2:
            subdomain = domain_parts[0]
            if subdomain in ['o408587', 'api', 'cdn', 'static', 'assets', 'media', 'img', 'images']:
                return False
        
        suspicious_domains = [
            'example.com', 'test.com', 'sample.com', 'dummy.com',
            'fake.com', 'invalid.com', 'placeholder.com'
        ]
        
        if domain in suspicious_domains:
            return False
        
        return True

    def normalize_phone(self, phone: str) -> str:
        if not phone:
            return ""
        
        clean_phone = re.sub(r'[\s\-\(\)]', '', phone)
        
        if clean_phone.startswith('0034'):
            clean_phone = '+34' + clean_phone[4:]
        elif clean_phone.startswith('34') and len(clean_phone) >= 11:
            clean_phone = '+34' + clean_phone[2:]
        elif not clean_phone.startswith('+34') and len(clean_phone) == 9:
            clean_phone = '+34' + clean_phone
            
        return clean_phone

    async def extract_contacts_from_page(self, page: Page, url: str) -> Tuple[List[str], List[str]]:
        try:
            response = await page.goto(url, timeout=20000, wait_until='domcontentloaded')
            
            if not response or response.status >= 400:
                return [], []
            
            await page.wait_for_timeout(2000)
            
            content = await page.content()
            
            raw_emails = self.email_pattern.findall(content.lower())
            valid_emails = []
            
            for email in raw_emails:
                if self.is_valid_email(email):
                    valid_emails.append(email)
            
            emails = list(dict.fromkeys(valid_emails))
            
            phones = []
            phone_matches = self.phone_pattern.findall(content)
            for match in phone_matches:
                if isinstance(match, tuple):
                    phone = ''.join(match)
                else:
                    phone = match
                phones.append(self.normalize_phone(phone))
            
            phones = list(set(phones))
            
            return emails, phones
            
        except Exception as e:
            logger.warning(f"Error extrayendo contactos de {url}: {str(e)}")
            return [], []

    async def verify_business_contacts_worker(self, business: Dict, business_index: int, total_businesses: int) -> Dict:
        async with self.semaphore:
            company_name = business.get('nombre', 'Sin nombre')
            website = business.get('sitio_web')
            current_phone = self.normalize_phone(business.get('telefono', ''))
            
            logger.info(f"[{business_index+1}/{total_businesses}] Procesando: {company_name}")
            
            result = business.copy()
            result['email'] = business.get('email', '')
            result['verification_results'] = {
                'emails_found': [],
                'phones_found': [],
                'email_verified': False,
                'phone_verified': False,
                'pages_checked': [],
                'error': None
            }
            
            if not website:
                result['verification_results']['error'] = 'No hay website'
                await self.update_progress(result, business_index, total_businesses)
                return result
            
            browser = None
            context = None
            page = None
            
            try:
                browser = await self.get_browser()
                
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True
                )
                
                page = await context.new_page()
                await page.set_extra_http_headers({
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
                })
                
                all_emails = set()
                all_phones = set()
                
                base_url = website.rstrip('/')
                
                for path in self.contact_paths:
                    try:
                        url = urljoin(base_url, path)
                        result['verification_results']['pages_checked'].append(url)
                        
                        emails, phones = await self.extract_contacts_from_page(page, url)
                        all_emails.update(emails)
                        all_phones.update(phones)
                        
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.warning(f"Error en {url}: {str(e)}")
                        continue
                
                result['verification_results']['emails_found'] = list(all_emails)
                result['verification_results']['phones_found'] = list(all_phones)
                
                current_email = result.get('email', '').lower() if result.get('email') else None
                
                if current_email and current_email in all_emails and self.is_valid_email(current_email):
                    result['verification_results']['email_verified'] = True
                
                if current_phone and current_phone in all_phones:
                    result['verification_results']['phone_verified'] = True
                
                if all_emails and not current_email:
                    valid_new_emails = [email for email in all_emails if self.is_valid_email(email)]
                    
                    if valid_new_emails:
                        new_email = valid_new_emails[0]
                        result['email'] = new_email
                        result['verification_results']['email_verified'] = True
                
                if all_phones and not current_phone:
                    new_phone = list(all_phones)[0]
                    result['telefono'] = new_phone
                    result['verification_results']['phone_verified'] = True
                
            except Exception as e:
                result['verification_results']['error'] = str(e)
                logger.error(f"Error procesando {company_name}: {str(e)}")
            
            finally:
                try:
                    if page:
                        await page.close()
                except:
                    pass
                
                try:
                    if context:
                        await context.close()
                except:
                    pass
                
                if browser:
                    await self.return_browser(browser)
            
            await self.update_progress(result, business_index, total_businesses)
            return result

    async def update_progress(self, result: Dict, business_index: int, total_businesses: int):
        with self.progress_lock:
            self.processed_count += 1
            self.results.append(result)
            
            if result.get('email') and result.get('verification_results', {}).get('email_verified'):
                self.verified_emails_count += 1
            if result.get('verification_results', {}).get('phone_verified'):
                self.verified_phones_count += 1

    def get_input_files(self) -> List[str]:
        script_dir = Path(__file__).parent
        data_dir = script_dir.parent / "data"
        pattern = data_dir / "negocios_*.json"
        return glob.glob(str(pattern))
    
    def get_processed_files(self) -> set:
        processed_files = set()
        script_dir = Path(__file__).parent
        data_dir = script_dir.parent / "data"
        pattern = data_dir / "email_*.json"
        
        email_files = glob.glob(str(pattern))
        for email_file in email_files:
            email_path = Path(email_file)
            original_name = email_path.name.replace("email_", "negocios_")
            original_path = str(data_dir / original_name)
            processed_files.add(original_path)
        return processed_files

    async def process_single_file(self, input_file: str):
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                businesses = json.load(f)
            
            if len(businesses) > 1:
                businesses = businesses[1:]
            else:
                logger.warning(f"Archivo {input_file} solo tiene {len(businesses)} elementos, saltando...")
                return
            
            logger.info(f"Procesando archivo: {input_file}")
            logger.info(f"Total negocios: {len(businesses)} (ignorando primer elemento)")
            
            self.processed_count = 0
            self.verified_emails_count = 0
            self.verified_phones_count = 0
            self.results = []
            
            await self.initialize_browsers()
            
            start_time = time.time()
            
            tasks = []
            for i, business in enumerate(businesses):
                task = asyncio.create_task(
                    self.verify_business_contacts_worker(business, i, len(businesses))
                )
                tasks.append(task)
                
                if len(tasks) >= self.max_concurrent_workers * 2:
                    await asyncio.sleep(1)
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            await self.cleanup_browsers()
            
            end_time = time.time()
            total_time = end_time - start_time
            
            output_file = str(Path(input_file).parent / Path(input_file).name.replace("negocios_", "email_"))
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            new_emails = sum(1 for b in self.results if b.get('email') and 
                           not businesses[self.results.index(b)].get('email'))
            
            logger.info(f"""
            Archivo {input_file} completado:
            - Negocios procesados: {len(businesses)}
            - Tiempo total: {int(total_time//60)}m {int(total_time%60)}s
            - Emails verificados: {self.verified_emails_count}
            - Teléfonos verificados: {self.verified_phones_count}
            - Emails nuevos encontrados: {new_emails}
            - Archivo guardado: {output_file}
            """)
            
        except Exception as e:
            logger.error(f"Error procesando archivo {input_file}: {str(e)}")
            await self.cleanup_browsers()

    async def process_all_files(self):
        input_files = self.get_input_files()
        processed_files = self.get_processed_files()
        
        files_to_process = [f for f in input_files if f not in processed_files]
        
        logger.info(f"Archivos encontrados: {len(input_files)}")
        logger.info(f"Archivos ya procesados: {len(processed_files)}")
        logger.info(f"Archivos a procesar: {len(files_to_process)}")
        
        if input_files:
            logger.info(f"Archivos de entrada: {input_files}")
        if processed_files:
            logger.info(f"Archivos procesados: {processed_files}")
        if files_to_process:
            logger.info(f"Archivos pendientes: {files_to_process}")
        
        if not files_to_process:
            logger.info("No hay archivos nuevos para procesar")
            return
        
        for input_file in files_to_process:
            logger.info(f"Iniciando procesamiento de: {input_file}")
            await self.process_single_file(input_file)
        
        logger.info("Todos los archivos han sido procesados")

async def main():
    MAX_WORKERS = 3
    MAX_BROWSERS = 2
    
    try:
        verifier = ConcurrentContactVerifier(
            max_concurrent_workers=MAX_WORKERS,
            max_browsers=MAX_BROWSERS
        )
        
        await verifier.process_all_files()
        
    except Exception as e:
        logger.error(f"Error general: {e}")

if __name__ == "__main__":
    if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(main())