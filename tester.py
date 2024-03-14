import asyncio
import os
import random
import re
import ssl
import pandas as pd
from bs4 import BeautifulSoup
import aiohttp


def load_existing_data(csv_file): # function to load existing data from the CSV file
    if os.path.exists(csv_file):
        existing_data = pd.read_csv(csv_file)
        return existing_data.drop_duplicates(subset=['URL']) # filter out duplicate entries based on URL
    else: # return empty FD
        return pd.DataFrame(
            columns=['English Title', 'Year', 'Country', 'Rating', 'Number of Raters', 'Actors', 'Synopsis',
                     'Number of Episode', 'URL']) 


async def fetch(session, movie_id): # function to fetch the page content
    url = f'https://www.viki.com/tv/{movie_id}c'
    async with session.get(url) as response:
        return await response.text()


async def scraper(movie_id, session, existing_data, scraped_data, processed_urls):
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
    ] # user agents
    await asyncio.sleep(random.uniform(1, 3)) # sleep for a random time
    headers = {'User-Agent': random.choice(user_agents)} # select a random user agent
    url = f'https://www.viki.com/tv/{movie_id}c' # construct the URL
    if url in processed_urls: # if the URL has already been processed,
        return
    async with session.get(url, headers=headers, timeout=30, allow_redirects=False) as response: # get request
        if response.status == 200: # if the request is successful
            result = await response.text() # get the content
            soup = BeautifulSoup(result, 'html.parser') # parse the content
            title_text = soup.find('h1') # find the title
            if title_text is None:
                return
            title = title_text.text 
            find_num = soup.find('span', {'class': 'sc-1n82s97-1 jKhckZ'}) # find the number of raters
            if find_num is None: # if the number of raters is not found,
                return
            find_num = soup.find('span', {'class': 'sc-1n82s97-1 jKhckZ'}).text # get the number of raters
            number_of_raters = int(re.sub(r'\D', '', find_num)) # extract the number of raters
            if number_of_raters < 5000: # if the number of raters is less than 5000,
                with open("404.txt", "a") as file:
                    file.write(f"{url}\n")
                return # return
            rating_text = soup.find('button', {'class': 'sc-1n82s97-2 fketVU'}).text # find the rating
            rating = float(re.search(r'\d+\.\d+', rating_text).group()) # extract the rating
            text_content = soup.find('div', {'class': 'sc-1wedfn3-2 eQsSdH'}) # find the text content
            text = text_content.text.strip() # extract the text content
            index = text.find('Synopsis') # find the index of the synopsis
            synopsis = text[index + len('Synopsis'):] # extract the synopsis
            country = soup.find('ol').find_all('a')[-1].text.strip() # extract the country
            year_str = soup.find('div', {'class': 'sc-q3f3t3-0 fgCTBq'}).text # extract the year
            if soup.find('button', {'class': 'sc-1l7dnk0-1 cvkKaZ'}) is None: # if the number of episodes is not found,
                return
            ep_str = soup.find('button', {'class': 'sc-1l7dnk0-1 cvkKaZ'}).text # extract the number of episodes
            ep = int(ep_str.split()[0]) # convert the number of episodes to an integer
            year = int(year_str) # convert the year to an integer
            if soup.find('div', {'class': 'sc-19mjesa-0 gfAAfH'}) is None: # if the cast list is not found,
                return
            cast_list = soup.find('div', {'class': 'sc-19mjesa-0 gfAAfH'}).text 
            names = [name for name in cast_list.split("Cast") if name] # extract the cast names
            actor_names = []
            if country == 'Mainland China': # if the country is Mainland China,
                country = 'China' # set the country to China
            for name in names: 
                actor_name = re.sub(r'(Main|Supporting)\s*', '', name).strip()
                actor_names.append(actor_name) 
            if movie_id not in existing_data['URL'].values: # if the movie is not already in the existing data,
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
                with open("processed.txt", "a") as file: # write the URL to the processed file
                    file.write(f"{url}\n") 
                scraped_data.append(movie_data)
        elif response.status == 302: # if the status code is 302,
            processed_urls = set()
            with open('302.txt', 'r') as file: # read the URLs from the 302 file
                for line in file:
                    processed_urls.add(line.strip())
            with open('404.txt', 'r') as file:
                for line in file:
                    processed_urls.add(line.strip())
            with open('processed.txt', 'r') as file:
                for line in file:
                    processed_urls.add(line.strip())
            if url not in processed_urls: # if the URL is not in the processed URLs,
                with open("302.txt", "a") as file: # write the URL to the 302 file
                    file.write(f"{url}\n")
        elif response.status == 404: # if the status code is 404,
            processed_urls = set()
            with open('404.txt', 'r') as file: 
                for line in file:
                    processed_urls.add(line.strip())
            if url not in processed_urls: # if the URL is not in the processed URLs,
                with open("404.txt", "a") as file:
                    file.write(f"{url}\n")
        else:
            with open("others.txt", "a") as file:
                file.write(f"{url}\n")


async def main(start_id, end_id, batch_size, processed_urls): # main function
    csv_file = 'movie_data.csv' # CSV file to store the data
    existing_data = load_existing_data(csv_file) # load the existing data

    connector = aiohttp.TCPConnector(ssl=False)
    urls_appended = 0

    async with aiohttp.ClientSession(connector=connector) as session: # create a session
        for batch_start in range(start_id, end_id, batch_size): # iterate over the range
            batch_end = min(batch_start + batch_size, end_id) # calculate the end of the batch
            scraped_data = [] # list to store the scraped data
            tasks = [scraper(movie_id, session, existing_data, scraped_data, processed_urls) for movie_id in
                     range(batch_start, batch_end)]
            await asyncio.gather(*[asyncio.create_task(task) for task in tasks])
            urls_appended += len(scraped_data)

            scraped_df = pd.DataFrame(scraped_data) # create a DataFrame from the scraped data
            if not scraped_df.empty: # if the DataFrame is not empty,
                scraped_df = scraped_df[~scraped_df['URL'].isin(existing_data['URL'])] # filter out existing URLs
                scraped_df.to_csv(csv_file, mode='a', index=False, header=not os.path.exists(csv_file),
                                  encoding='utf-8') # append the data to the CSV file

    return urls_appended

if __name__ == '__main__':
    processed_urls = set()
    with open('404.txt', 'r') as file:
        for line in file:
            processed_urls.add(line.strip())
    with open('processed.txt', 'r') as file:
        for line in file:
            processed_urls.add(line.strip())

    start_id = 1
    end_id = 40500
    batch_size = 1000

    total_urls_appended = asyncio.run(main(start_id, end_id, batch_size, processed_urls))
    print(f"Total URLs appended: {total_urls_appended}")
