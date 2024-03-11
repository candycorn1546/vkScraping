# Overview


- ## Purpose
  Viki is one of Asia's biggest movie websites, with millions of reviews about different movies and shows. VK holds valuable insights and trends in movie ratings, user reviews, and demographic information by scraping VK and comparing it with MDL and DB, the world's most popular foreign drama website. By systematically comparing DB, MDL, and VK data, a comprehensive and global perspective on audience preferences, cultural influences, and viewing habits can be derived. This comparative analysis enriches our understanding of the entertainment landscape and sheds light on the cross-cultural dynamics shaping the reception of movies and shows.

  ## Method
    - First, it uses the tester.py to run the 40k URL and retry to parse as many as possible into the CSV file. Usually, a program doesn't like it when somebody sends a lot of traffic to a server, therefore it's going to give a lot of 302 errors and tries to redirect the program. Whenever the tester receives the 302 error, it's going to write it into a txt file called '302.txt.'
    - After finishing running the tester.py, now the 'writer.py' is the main program. The 'writer.py' goes through the set of '302.txt' URLs and tries and tries again until it either gets a 404 ERROR or gets a 200, there will be another program called 'cleaner' which removes all the URLs in 404.txt from 302.txt, which means the program won't test again because it's a 404 ERROR.
    - The program will keep running until it completely clears the entire txt file, the 'cleaner.py' ensures that if a URL is already processed then it will be removed from the '302.txt' file which prevents the writer from trying again.
  
  ## Visuals
  
<img width="1501" alt="Screenshot 2024-03-10 at 9 18 14 PM" src="https://github.com/candycorn1546/vkScraping/assets/157404986/354e8c7f-c9d9-4c94-8462-aa878b5dfe3f">
<img width="1500" alt="Screenshot 2024-03-10 at 9 18 29 PM" src="https://github.com/candycorn1546/vkScraping/assets/157404986/c82a415b-138c-42b7-8069-92a1137a4943">
<img width="1479" alt="Screenshot 2024-03-10 at 9 18 36 PM" src="https://github.com/candycorn1546/vkScraping/assets/157404986/706dbf55-94a0-4421-b63b-3041bb5b3ae1">
<img width="1491" alt="Screenshot 2024-03-10 at 9 18 42 PM" src="https://github.com/candycorn1546/vkScraping/assets/157404986/a07c134d-e382-4b90-86d1-8f0466fa3ed6">
