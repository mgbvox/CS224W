"""
A scraper script for grabbing the latest course information from https://snap.stanford.edu/class/cs224w-2023/

Outputs to ./course
"""

import re
from typing import AsyncGenerator
import httpx
import aiofiles
from bs4 import BeautifulSoup, Tag
import asyncio
from urllib.parse import urljoin, urlsplit
from pathlib import Path



# use latest class with full content
BASE_URL: str = "https://snap.stanford.edu/class/cs224w-2023/"
GID_PAT = re.compile(r"[\w-]{26,}")

def find_root(root: Path) -> Path:
    if (root / "pyproject.toml").exists():
        return root
    else:
        return find_root(root.parent)

async def fetch(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url)
    response.raise_for_status()
    return response.text

async def download_file(client: httpx.AsyncClient, url: str, dest_path: Path) -> None:
    print(f"{url} --> {dest_path}")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.suffix == ".html":
        content = f"#VISIT WEBPAGE:\n\n[{url}]({url})".encode("utf-8")
        dest_path = dest_path.with_suffix(".md")
    else:
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            content = response.content
        except httpx.HTTPStatusError:
            content = f"#INVALID LINK / VISIT DIRECTLY:\n\n[{url}]({url})".encode("utf-8")
            dest_path = dest_path.with_suffix(".md")


    async with aiofiles.open(dest_path, 'wb') as f:
        await f.write(content)

async def aiter_from_list[T](lst: list[T]) -> AsyncGenerator[T, None]:
    for item in lst:
        yield item

async def bulk_download(links: list[Tag | tuple[str, str]], to: Path, client: httpx.AsyncClient,) -> None:
    async for link in aiter_from_list(links):
        if not to.exists():
            to.mkdir(parents=True, exist_ok=True)
        match link:
            case (url, name):
                await download_file(client, url, to / name)
            case Tag():
                url: str = urljoin(BASE_URL, link['href'])
                name: str = Path(url).name
                if not Path(url).suffix:
                    # probably is a link to an html article - derive name this way:
                    name:str = Path(urlsplit(url).path).name + ".html"

                await download_file(client, url, to / name)

async def process_row(row: Tag,
                      outdir: Path,
                      client: httpx.AsyncClient,) -> None:
    cells = row.find_all('td')
    if len(cells) < 2:
        return

    # Extract information from each row
    description: str = cells[1].get_text(strip=True)
    description = re.sub(r'^(\d+)\.\s*', r'\1-', description)  # Remove digit-dots at the beginning
    description = re.sub(r'\[.*?]', '', description)  # Remove [{link_text}] annotations
    description = description.replace('\n', ' ').strip().replace('/', '-').replace(' ', "_").lower()
    directory: Path = outdir / "course" / Path(description)
    directory.mkdir(parents=True, exist_ok=True)
    print(directory)

    # Download slides
    slides_dir: Path = directory / "slides"
    slides_links = cells[1].find_all('a', href=True)
    await bulk_download(slides_links, to=slides_dir, client=client)

    # Download optional readings
    reading_dir: Path = directory / "reading"
    reading_links = cells[2].find_all('a', href=True) if len(cells) > 2 else []
    await bulk_download(reading_links, to=reading_dir, client=client)

    # Download homework and colab notebooks
    homework_dir: Path = directory / "homework"
    event_links = cells[3].find_all('a', href=True) if len(cells) > 3 else []

    hw_links = []
    colabs = 0
    for link in event_links:
        event_url = link['href']
        # Convert colab links to downloadable IPYNB files
        if 'colab.research.google.com' in event_url:
            gid = GID_PAT.findall(event_url)
            if len(gid) != 1:
                print(f"Invalid download link: {event_url}")
                continue
            gid = gid[0]
            event_url = f"https://docs.google.com/uc?export=download&id={gid}"
            event_name = f"CS224W_Colab_{colabs}.ipynb"
            colabs += 1

            hw_links.append((event_url, event_name))
        else:
            hw_links.append(link)
    await bulk_download(hw_links, to=homework_dir, client=client)

    # if row dir is empty, remove it:
    if not list(directory.iterdir()):
        directory.rmdir()



async def main() -> None:
    async with httpx.AsyncClient() as client:
        page_html: str = await fetch(client, BASE_URL)
        outdir: Path = find_root(Path(__file__).parent)

        soup = BeautifulSoup(page_html, 'html.parser')
        table = soup.find('table', class_='table')
        if not table:
            print("Table not found.")
            return

        rows = table.find('tbody').find_all('tr')
        await asyncio.gather(*[process_row(row, outdir, client) for row in rows])

if __name__ == "__main__":
    asyncio.run(main())
