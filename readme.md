# Reddit scraper

#### Important notice regarding the data and model decay

This scraper uses data from the Cornell Convokit corpus (2005-2019). My own experience shows that classifiers and generators trained using this data work perfectly as of 2022. 

If you need data from the last 3 years, you will need to use `snscrape` and `requests`. I will add this functionality later when I have more free time. 

Still, consider using this script for old data as it is several orders of magnitudes faster than scraping from scratch and can give you basic statistics to organize your scraping effort.  

### engine.py

```
import engine

if __name__ == '__main__':
    engine.get_reddit_dfs(subreddits=[0,200], number_children=5, max_comments_per_subreddit=1000000)

```
#### function get_reddit_dfs

- *subreddits* (list): Range of subreddits to parse (all existing subreddits ordered by size). `[0, 10]` gets the top 10. Alternatively, list of `int` pointing to subreddits per their index number in link_df.csv (e.g., `[1, 3, 6, 19, 53, 23, 78]`)
- *top_OP_denominator* (int): Calculate which original posts to keep based on upvotes. 2 means top 1/2 (or top 50%), 4 means top 1/4 (or top 25%), and so on. This is useful as most parent posts have no content or child comments. If you want to keep the worst posts, use a negative number in the function.
- *number_children* (int): How far down to go in the comment tree. 0 => keeps only the parents, 10 => gets up to 10 children.
- *max_comments_per_subreddit* (int): maximum number of comments to get per subreddit. Useful if you run out of RAM or if you want diversity and don't want the top subreddits to dominate your dataset. 
- *max_time_since_OP* (int): Keep comments after `n` number of hours since the original post. Useful if you want to study reactions within a timeframe as attention and exposure decrease logarithmically.
- *min_length_comment* (int): Discard comments whose length using the `.split()` method is lower than this number. 
- *min_length_OP* (int): Discard original posts whose length using the `.split()` method is lower than this number (Recommended value = 0 as most have no text content).
- *save_dir* (str): save directory

Download and parse JSON data using the constraints laid out above. Outputs a comma-separated file  containing `id, text, root, time_since_OP, upvotes, reply_to`. There is additional metadata you can save by modifying the function. Please see: https://convokit.cornell.edu/documentation/subreddit.html

### build_links_dataframe.py

Get the full list of subreddits ordered by size. The file in this repo has the first 100,000.

### Other Notices

#### Notice regarding the script

The purpose of this script is to parse a large number of Reddit comments. If you are interested in a single subreddit, conversational dynamics, or qualitative analysis, consider using the original Convokit library, which is prettier and has more functionalities: https://convokit.cornell.edu/documentation/

#### Notice regarding your OS

Your computer must be able to execute Bash commands using Python. If you use Windows, please use WSL or see https://stackoverflow.com/questions/58402900/how-to-run-bash-commands-using-subprocess-run-on-windows

#### Notice regarding optimization

My computer only has 48 GB of RAM, so I had to compromise speed and do things iteratively. The script can parse several TB of data in a few hours, but if you have more RAM than I do, there are ways to speed up the process. 

#### My own experience with this data

This is an interesting corpus because it's a snapshot of the internet before the catastrophe hit in 2020 and, later, the events in January 2021 in the US, both of which resulted in stringent moderation. 

My use cases were user engagement, IR, and text classification, and my models generalize better when you skip 2020-2021 and augment using comments from 2022. 

## License

Attribution-NonCommercial 3.0 Unported (CC BY-NC 3.0)

https://creativecommons.org/licenses/by-nc/3.0/
