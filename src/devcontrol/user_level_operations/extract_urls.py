from bs4 import BeautifulSoup
import re
from urllib.request import urlopen


def extract_990_urls(html_content):
    """
    Extract download URLs from IRS 990 downloads page content
    Args:
        html_content (str): Raw HTML content from the IRS page
    Returns:
        list: List of download URLs
    """
    urls = []
    soup = BeautifulSoup(html_content, 'html.parser')

    # Look for links that match the 990 download pattern
    for link in soup.find_all('a', href=True):
        href = link['href']
        # Look for links to zip files in the epostcard/990 path
        if 'pub/epostcard/990/xml/2024' in href and href.endswith('.zip'):
            if not href.startswith('http'):
                href = 'https://apps.irs.gov' + href
            urls.append(href)

    return urls


def get_page_content(page_url):
    with urlopen(page_url) as response:
        html = response.read().decode('utf-8')
        return html


def get_urls_referenced_on_website(site):
    html = get_page_content(site)
    urls = extract_990_urls(html)
    with open('/home/don/Documents/Temp/dev990/xml_urls.txt', 'w') as f:
        for line in urls:
            print(line)
            f.write(f"{line}\n")
    return urls


if __name__ == '__main__':
    url = 'https://www.irs.gov/charities-non-profits/form-990-series-downloads'
    html = get_page_content(url)
    urls = extract_990_urls(html)
    with open('/home/don/Documents/Temp/xml_urls.txt', 'w') as f:
        for line in urls:
            print(line)
            f.write(f"{line}\n")