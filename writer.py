import asyncio
import random
import logging
import os
import re
import ssl
import time
import certifi
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


def load_existing_data(csv_file): # function to load existing data from CSV file
    if os.path.exists(csv_file):
        existing_data = pd.read_csv(csv_file)
        return existing_data.drop_duplicates(subset=['URL'])
    else:
        return pd.DataFrame(
            columns=['English Title', 'Year', 'Country', 'Rating', 'Number of Raters', 'Actors', 'Synopsis',
                     'Number of Episode', 'URL'])


async def get_movie_info(url, session, csv_file): # function to get movie information
    processed_urls = set()
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/96.0.1054.62",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
    ]
    await asyncio.sleep(random.uniform(5, 10))
    while True: # infinite loop
        headers = {'User-Agent': random.choice(user_agents)} # select a random user agent
        try: # try block
            async with session.get(url, headers=headers, allow_redirects=False) as response:
                logging.info(f"Requesting URL: {url}, Status Code: {response.status}")
                if response.status == 200:
                    print(f"200, {url}")
                    result = await response.text()
                    soup = BeautifulSoup(result, 'html.parser')
                    title_text = soup.find('h1')
                    if title_text is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    title = title_text.text
                    find_num = soup.find('span', {'class': 'sc-1n82s97-1 jKhckZ'})
                    if find_num is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    find_num = soup.find('span', {'class': 'sc-1n82s97-1 jKhckZ'}).text
                    number_of_raters = int(re.sub(r'\D', '', find_num))
                    if number_of_raters < 5000:
                        print(f"Number of raters less than 5000 for URL: {url}")
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    if soup.find('button', {'class': 'sc-1n82s97-2 fketVU'}) is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    rating_text = soup.find('button', {'class': 'sc-1n82s97-2 fketVU'}).text
                    rating = float(re.search(r'\d+\.\d+', rating_text).group())
                    text_content = soup.find('div', {'class': 'sc-1wedfn3-2 eQsSdH'})
                    if text_content is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    text = text_content.text.strip()
                    index = text.find('Synopsis')
                    if index == -1:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    synopsis = text[index + len('Synopsis'):]
                    country = soup.find('ol').find_all('a')[-1].text.strip()
                    if soup.find('div', {'class': 'sc-q3f3t3-0 fgCTBq'}) is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    year_str = soup.find('div', {'class': 'sc-q3f3t3-0 fgCTBq'}).text
                    if soup.find('button', {'class': 'sc-1l7dnk0-1 cvkKaZ'}) is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    ep_str = soup.find('button', {'class': 'sc-1l7dnk0-1 cvkKaZ'}).text
                    ep = int(ep_str.split()[0])
                    year = int(year_str)
                    if soup.find('div', {'class': 'sc-19mjesa-0 gfAAfH'}) is None:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                        return
                    cast_list = soup.find('div', {'class': 'sc-19mjesa-0 gfAAfH'}).text
                    names = [name for name in cast_list.split("Cast") if name]
                    actor_names = []
                    for name in names:
                        actor_name = re.sub(r'(Main|Supporting)\s*', '', name).strip()
                        actor_names.append(actor_name)
                    movie_data = {
                        'English Title': title,
                        'Year': year,
                        'Country': country,
                        'Rating': rating,
                        'Number of Raters': number_of_raters,
                        'Actors': actor_names,
                        'Synopsis': synopsis,
                        'Number of Episode': ep,
                        'URL': url
                    }
                    existing_data = pd.read_csv(csv_file)
                    if url not in existing_data['URL'].values:
                        print(f"Processed URL: {title}")
                        with open(csv_file, "a") as file:
                            pd.DataFrame([movie_data]).to_csv(file, header=False, index=False)
                        with open("processed.txt", "a") as file:
                            file.write(f"{url}\n")
                    else:
                        print(f"URL already exists: {url}")
                        with open("processed.txt", "a") as file:
                            file.write(f"{url}\n")
                    return movie_data
                elif response.status == 404:
                    print("404")
                    processed_urls = set()
                    with open('404.txt', 'r') as file:
                        for line in file:
                            processed_urls.add(line.strip())
                    if url not in processed_urls:
                        with open("404.txt", "a") as file:
                            file.write(f"{url}\n")
                    return None

        except aiohttp.ClientConnectorError as e: # except block
            logging.error(f"Error connecting to server: {e}") 
        except aiohttp.client_exceptions.ServerDisconnectedError as e: # except block
            logging.error(f"Server disconnected while processing URL: {url}")
        except Exception as e: # except block
            logging.error(f"Error processing URL {url}: {e}")

    return None


async def fetch_urls(urls, session, csv_file): # function to fetch URLs
    tasks = [get_movie_info(url, session, csv_file) for url in urls]
    return await asyncio.gather(*tasks)


async def scrape_unique_urls(text_file, csv_file): # function to scrape unique URLs
    unique_urls = set()

    with open(text_file, 'r') as file: # open the file in read mode
        for line in file:
            url = line.strip()
            unique_urls.add(url)

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        movie_data = await fetch_urls(unique_urls, session, csv_file)
        return movie_data


async def main():
    start_time = time.time()
    text_file = '302.txt'
    csv_file = 'movie_data.csv'
    movie_data = await scrape_unique_urls(text_file, csv_file)
    end_time = time.time()
    print(f"Scraping completed in {end_time - start_time} seconds.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("aiohttp.client").setLevel(logging.INFO)  # Set logging level for aiohttp.client to INFO
    asyncio.run(main())
