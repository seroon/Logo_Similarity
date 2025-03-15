import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import parse_parquet  
from multiprocessing import Pool, cpu_count



def get_logo_url(domain):
    
    base_url = f"https://{domain}"

    try:
        response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        for img_tag in soup.find_all("img", src=True):
            src = img_tag["src"]
            alt_text = img_tag.get("alt", "").lower()
            class_list = " ".join(img_tag.get("class", [])).lower()

        
            if "logo" in src.lower() or "logo" in alt_text or "logo" in class_list:
                return urljoin(base_url, src)

        return None

    except requests.RequestException as e:
        print(f"Eroare la accesarea {domain}: {e}")
        return None


def download_logo(url, output_dir="logos_test"):
  
    if not url:
        return None

    try:
        response = requests.get(url, stream=True, timeout=5)
        response.raise_for_status()

        os.makedirs(output_dir, exist_ok=True)  

        file_extension = os.path.splitext(urlparse(url).path)[1]  # Păstrează extensia originală
        domain_name = urlparse(url).netloc.replace(".", "_")
        file_path = os.path.join(output_dir, f"{domain_name}{file_extension}")

        with open(file_path, "wb") as file:
            file.write(response.content)

        print(f"Logo descărcat: {file_path}")
        return file_path

    except requests.RequestException as e:
        print(f"Eroare la descărcarea logo-ului de la {url}: {e}")
        return None


def process_domain(domain):
    
    print(f"Procesare: {domain}")
    logo_url = get_logo_url(domain)

    if logo_url:
        print(f"Logo găsit pentru {domain}: {logo_url}")
        download_logo(domain, logo_url)
    else:
        print(f"Logo nu a fost găsit pentru {domain}.")


def process_domains_multiprocessing(parquet_file):
    
    domains = parse_parquet.extract_domains_from_parquet(parquet_file)

    with Pool(cpu_count()) as pool:
        pool.map(process_domain, domains)


if __name__ == "__main__":
    parquet_file = "logos.snappy.parquet"
    process_domains_multiprocessing(parquet_file)
