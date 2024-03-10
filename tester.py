import asyncio

import asyncio
import os
import random
import re
import ssl

import pandas as pd
import requests
from bs4 import BeautifulSoup
import aiohttp


def load_existing_data(csv_file):  # function to load existing data from the CSV file
    if os.path.exists(csv_file):
        existing_data = pd.read_csv(csv_file)
        return existing_data.drop_duplicates(subset=['URL'])  # Filter out duplicate entries based on URL
    else:
        return pd.DataFrame(
            columns=['English Title', 'Year', 'Country', 'Rating', 'Number of Raters', 'Actors', 'Synopsis',
                     'Number of Episode', 'URL'])


async def fetch(session, movie_id):  # function to fetch the page content
    url = f'https://www.viki.com/tv/{movie_id}c'  # construct the URL
    async with session.get(url) as response:  # get request
        return await response.text()  # return the content
async def scraper(movie_id, session, existing_data, scraped_data,processed_urls):
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
    ]  # user agents
    await asyncio.sleep(random.uniform(1, 3))  # sleep for a random time
    headers = {'User-Agent': random.choice(user_agents)}
    url = f'https://www.viki.com/tv/{movie_id}c'  # construct the URL
    if url in processed_urls:
        return
    async with session.get(url, headers=headers, timeout=30,allow_redirects=False) as response:
        #print(response.status)
        if response.status == 200:
            print(f"{movie_id} IN")
            result = await response.text()  # get request (await response.text() instead of requests.get())
            soup = BeautifulSoup(result, 'html.parser')  # parse the page
            title_text = soup.find('h1')  # find the title
            if title_text is None:
                print(f"{movie_id} OUT")
                return
            title = title_text.text
            find_num = soup.find('span', {'class': 'sc-1n82s97-1 jKhckZ'})  # find the number of raters
            if find_num is None:
                print(f"{movie_id} OUT")
                return
            find_num = soup.find('span', {'class': 'sc-1n82s97-1 jKhckZ'}).text  # find the number of raters
            number_of_raters = int(re.sub(r'\D', '', find_num))  # remove non-digits
            if number_of_raters < 5000:
                print(f"Number of raters less than 5000 for URL: {url}")
                with open("404.txt", "a") as file:
                    file.write(f"{url}\n")
                return  # Skip the movie if there are no raters
            rating_text = soup.find('button', {'class': 'sc-1n82s97-2 fketVU'}).text  # find the rating
            rating = float(re.search(r'\d+\.\d+', rating_text).group())  # extract the rating
            text_content = soup.find('div', {'class': 'sc-1wedfn3-2 eQsSdH'})  # find the text content
            text = text_content.text.strip()
            index = text.find('Synopsis')
            synopsis = text[index + len('Synopsis'):]  # extract the synopsis
            country = soup.find('ol').find_all('a')[-1].text.strip()  # find the country
            year_str = soup.find('div', {'class': 'sc-q3f3t3-0 fgCTBq'}).text  # find the year
            if soup.find('button', {'class': 'sc-1l7dnk0-1 cvkKaZ'}) is None:
                print(f"{movie_id} OUT")
                return
            ep_str = soup.find('button', {'class': 'sc-1l7dnk0-1 cvkKaZ'}).text  # find the number of episodes
            ep = int(ep_str.split()[0])  # extract the number of episodes
            year = int(year_str)
            if soup.find('div', {'class': 'sc-19mjesa-0 gfAAfH'}) is None:
                print(f"{movie_id} OUT")
                return
            cast_list = soup.find('div', {'class': 'sc-19mjesa-0 gfAAfH'}).text
            names = [name for name in cast_list.split("Cast") if name]
            actor_names = []
            if country == 'Mainland China' or 'China' in country:
                country = 'China'
            if country == 'Corea del Sur' or 'Coreia' or 'Corée':
                country = 'Korea'
            for name in names:
                actor_name = re.sub(r'(Main|Supporting)\s*', '', name).strip()
                actor_names.append(actor_name)
            if movie_id not in existing_data['URL'].values:
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
                print(f"{title} SUCCESS")
                with open("processed.txt", "a") as file:
                    file.write(f"{url}\n")
                scraped_data.append(movie_data)  # Append movie data only if it's defined
        elif response.status == 302:
            processed_urls = set()
            with open('302.txt', 'r') as file:
                for line in file:
                    processed_urls.add(line.strip())
            with open('404.txt', 'r') as file:
                for line in file:
                    processed_urls.add(line.strip())
            with open('processed.txt', 'r') as file:
                for line in file:
                    processed_urls.add(line.strip())
            if url not in processed_urls:
                with open("302.txt", "a") as file:
                    file.write(f"{url}\n")
        elif response.status == 404:
            print('404')
            processed_urls = set()
            with open('404.txt', 'r') as file:
                for line in file:
                    processed_urls.add(line.strip())
            if url not in processed_urls:
                with open("404.txt", "a") as file:
                    file.write(f"{url}\n")
        else:
            with open("others.txt", "a") as file:
                file.write(f"{url}\n")


async def main(start_id, end_id, batch_size, processed_urls):
    csv_file = 'movie_data.csv'
    existing_data = load_existing_data(csv_file)

    connector = aiohttp.TCPConnector(ssl=False)
    urls_appended = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        for batch_start in range(start_id, end_id, batch_size):
            batch_end = min(batch_start + batch_size, end_id)
            print(f"Processing batch {(batch_start - start_id) // batch_size + 1}...")
            scraped_data = []
            tasks = [scraper(movie_id, session, existing_data, scraped_data, processed_urls) for movie_id in
                     range(batch_start, batch_end)]
            await asyncio.gather(*[asyncio.create_task(task) for task in tasks])
            urls_appended += len(scraped_data)  # Update the count of appended URLs

            # Convert the list of scraped data to a DataFrame
            scraped_df = pd.DataFrame(scraped_data)
            if not scraped_df.empty:  # Check if scraped_df is empty
                scraped_df = scraped_df[~scraped_df['URL'].isin(existing_data['URL'])]

                # Append the scraped data to the existing CSV file, without duplicates
                scraped_df.to_csv(csv_file, mode='a', index=False, header=not os.path.exists(csv_file),
                                  encoding='utf-8')

    return urls_appended

def remove_duplicates(text_file):
    with open(text_file, "r") as file:
        lines = file.readlines()

    # Remove duplicates while preserving order
    unique_lines = list(dict.fromkeys(lines))

    # Open the file in write mode and overwrite with unique links
    with open(text_file, "w") as file:
        file.writelines(unique_lines)

def remove_urls():
    with open('404.txt', 'r') as file:
        urls_to_remove = {line.strip() for line in file}
    with open('processed.txt', 'r') as file:
        urls_to_remove.update({line.strip() for line in file})

    with open('302.txt', 'r') as file:
        lines = file.readlines()

    filtered_lines = [line for line in lines if line.strip() not in urls_to_remove]

    with open('302.txt', 'w') as file:
        file.writelines(filtered_lines)
    movie_data = pd.read_csv('movie_data.csv')
    urls_to_keep = set(movie_data['URL'])

    # Read URLs from 302.txt
    with open('302.txt', 'r') as file:
        lines = file.readlines()

    # Filter out the URLs present in movie_data.csv
    filtered_lines = [line for line in lines if line.strip() not in urls_to_keep]

    # Write filtered URLs back to 302.txt
    with open('302.txt', 'w') as file:
        file.writelines(filtered_lines)

if __name__ == '__main__':
    processed_urls = set()
    with open('404.txt', 'r') as file:
        for line in file:
            processed_urls.add(line.strip())
    with open('processed.txt', 'r') as file:
        for line in file:
            processed_urls.add(line.strip())

    start_id = 35000  # Adjust the start_id as per your requirement
    end_id = 40500  # Adjust the end_id as per your requirement
    batch_size = 50  # Adjust the batch size as per your requirement

    total_urls_appended = asyncio.run(main(start_id, end_id, batch_size, processed_urls))
    print(f"Total URLs appended: {total_urls_appended}")
    # remove_duplicates("302.txt")
    # remove_duplicates("404.txt")
    # remove_urls()


