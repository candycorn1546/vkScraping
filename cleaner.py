import os
import time
import pandas as pd


def remove_duplicates(text_file): # function to remove duplicates from a text file
    with open(text_file, "r") as file:
        lines = file.readlines()

    unique_lines = list(dict.fromkeys(lines))

    with open(text_file, "w") as file:
        file.writelines(unique_lines)


def remove_urls(): # function to remove URLs
    counter = 0 # initialize counter
    while counter < 1000: # while loop
        with open('404.txt', 'r') as file: # open the file in read mode
            urls_to_remove = {line.strip() for line in file}
        with open('processed.txt', 'r') as file:
            urls_to_remove.update({line.strip() for line in file})
        movie_data = pd.read_csv('movie_data.csv')
        urls_to_remove.update(set(movie_data['URL'])) # update the set with the URLs from the CSV file

        with open('302.txt', 'r') as file:
            lines = file.readlines()

        before_size = os.path.getsize('302.txt') # get the size of the file

        filtered_lines = [line for line in lines if line.strip() not in urls_to_remove] # filter out the URLs to remove

        with open('302.txt', 'w') as file: # open the file in write mode
            file.writelines(filtered_lines)

        after_size = os.path.getsize('302.txt') # get the size of the file after removing the URLs
        different = before_size - after_size # calculate the difference in size
        print(f"Size difference: {different} bytes") # print the difference in size

        counter += 1 # increment the counter
        if counter % 10 == 0:
            print(f"Iteration {counter} completed.") # print the iteration number
        time.sleep(10)


if __name__ == '__main__':
    remove_duplicates("302.txt")
    remove_duplicates("404.txt")
    remove_duplicates("processed.txt")
    remove_urls()
