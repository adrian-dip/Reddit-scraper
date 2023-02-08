from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import re
import pandas as pd

if __name__ == '__main__':


    # build directory of subreddits

    req = Request("https://zissou.infosci.cornell.edu/convokit/datasets/subreddit-corpus/corpus-zipped/")
    html_page = urlopen(req)

    soup = BeautifulSoup(html_page, "lxml")
    text = []
    links = []
    for link in soup.findAll('a'):
        if link.get('href') != '../':
            links.append(link.get('href'))
            text.append(link.next_sibling)


    # construct links to subreddits from directory

    full_links = []
    texts = []
    for parent in links:
        example = "https://zissou.infosci.cornell.edu/convokit/datasets/subreddit-corpus/corpus-zipped/" + parent
        req = Request(example)
        html_page = urlopen(req)
        soup = BeautifulSoup(html_page, "lxml")

    for link in soup.findAll('a'):
        if link.get('href') != '../':
            full_links.append(example + link.get('href'))
            texts.append(link.next_sibling)


    # get corpus size in bytes

    sizes = []
    for el in texts:
        el = el[:-2]
        sizes.append(int(re.findall('[0-9]+', el)[-1]))


    # get subreddit names

    names = []
    for l in full_links:
        s = re.findall('(?<=/)(\w+)', l)[-1]
        names.append(s)



    # sort by size and save

    link_df = pd.DataFrame(list(zip(full_links, sizes, names)), columns=['Link', 'Size', 'Name'])
    link_df.sort_values(by=['Size'], ascending=False, inplace=True)
    link_df.to_csv('link_df.csv', index=False)