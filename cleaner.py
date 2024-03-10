import os
import time

import pandas as pd


def remove_duplicates(text_file):
    with open(text_file, "r") as file:
        lines = file.readlines()

    # Remove duplicates while preserving order
    unique_lines = list(dict.fromkeys(lines))

    # Open the file in write mode and overwrite with unique links
    with open(text_file, "w") as file:
        file.writelines(unique_lines)


def remove_urls():
    counter = 0  # Initialize counter
    while True:
        with open('404.txt', 'r') as file: # Open file in read mode
            urls_to_remove = {line.strip() for line in file} # Create a set of URLs to remove
        with open('processed.txt', 'r') as file: # Open file in read mode
            urls_to_remove.update({line.strip() for line in file}) # Update set with URLs to remove
        movie_data = pd.read_csv('movie_data.csv') # Read CSV file
        urls_to_remove.update(set(movie_data['URL'])) # Update set with URLs to remove

        with open('302.txt', 'r') as file: # Open file in read mode
            lines = file.readlines() # Read lines from file

        before_size = os.path.getsize('302.txt') # Get size before removing URLs

        filtered_lines = [line for line in lines if line.strip() not in urls_to_remove] # Filter out URLs to remove

        with open('302.txt', 'w') as file: # Open file in write mode
            file.writelines(filtered_lines) # Write filtered lines to file

        after_size = os.path.getsize('302.txt') # Get size after removing URLs
        different = before_size - after_size
        print(f"Size difference: {different} bytes")

        counter += 1  # Increment counter
        if counter % 10 == 0:
            print(f"Iteration {counter} completed.")  # Print current iteration
        time.sleep(10)


if __name__ == '__main__':
    remove_duplicates("302.txt")
    remove_duplicates("404.txt")
    remove_duplicates("processed.txt")
    remove_urls()
