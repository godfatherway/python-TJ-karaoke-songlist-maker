import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
import time

# Retry decorator with exponential backoff strategy
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def get_url(url):
    return requests.get(url)

# Function to connect to the web page and scrape data
def tjmediasongnumberfindingaddressmaker(x):
    print(f"Searching for song with query parameter: {x}")
    # Send a GET request to the TJ Media website with the given query parameter
    req = get_url('https://www.tjmedia.com/tjsong/song_search_list.asp?strType=16&natType=&strText='+str(x)+'&strCond=1&strSize05=100')
    
    # Decode the content of the response to UTF-8 encoding
    html = req.content.decode('utf-8', 'replace')
    
    # Create a BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(html, 'lxml')
    
    # Extract song information from the parsed HTML
    songnumber = soup.select('#BoardType1 > table > tbody > tr:nth-child(2) > td:nth-child(1) > span')
    
    # If no songs found, return None
    if not songnumber:
        print(f"No songs found for query parameter: {x}")
        return None
    
    songname = soup.select('#BoardType1 > table > tbody > tr:nth-child(2) > td.left')
    singername = soup.select('#BoardType1 > table > tbody > tr:nth-child(2) > td:nth-child(3)')
    lyricist = soup.select('#BoardType1 > table > tbody > tr:nth-child(2) > td:nth-child(4)')
    composer = soup.select('#BoardType1 > table > tbody > tr:nth-child(2) > td:nth-child(5)')

    # Extract text from HTML tags and store them in lists
    list1 = [data1.text for data1 in songnumber]
    list2 = [data2.text for data2 in songname]
    list3 = [data3.text for data3 in singername]

    # Combine the lists into a single list
    sets = list(zip(list1, list2, list3))
    
    print(f"Found {len(sets)} songs matching the query parameter: {x}")
    return sets

# Function to periodically write data to an Excel file
def write_to_excel(data, file_name):
    df = pd.DataFrame(data, columns=['Song Number', 'Song Name', 'Singer Name'])
    df.to_excel(file_name, index=False)

if __name__ == "__main__":
    # Create a Pool of processes for parallel processing
    pool = Pool()
    
    # Use the Pool to map the function over the range of values
    # This will execute the function tjmediasongnumberfindingaddressmaker in parallel for each value in the range
    print("Starting parallel processing...")
    file_name = 'all_songs.xlsx'
    datasets = []
    for result in pool.imap_unordered(tjmediasongnumberfindingaddressmaker, range(1, 99999)):
        if result:
            datasets.extend(result)
            write_to_excel(datasets, file_name)
    print("Parallel processing complete!")
    
    # Convert the combined data into a DataFrame
    df = pd.DataFrame(datasets, columns=['Song Number', 'Song Name', 'Singer Name'])
    
    # Write the DataFrame to a single Excel file
    df.to_excel(file_name, index=False)
    print(f"Excel file '{file_name}' created with all songs.")
