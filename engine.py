from os import listdir
import os
import json
import glob
import pandas as pd
import subprocess
import gc

link_df = pd.read_csv('link_df.csv')

def get_reddit_dfs(subreddits=None, 
                    top_OP_denominator=10, 
                    number_children=3, 
                    max_comments_per_subreddit=100000, 
                    max_time_since_OP=24, 
                    min_length_comment=5, 
                    min_length_OP=0, 
                    save_dir="", 
                    get_subreddits_sequentially=True,
                    link_df=None):


  assert type(subreddits) == list, "subreddits argument must be a list"
  assert type(top_OP_denominator) == int, "top_OP_denominator must be int"
  assert type(number_children) == int, "number_children must be int"
  assert type(max_comments_per_subreddit) == int, "max_comments_per_subreddit must be int"
  assert type(max_time_since_OP) == int, "max_time_since_OP must be int"
  assert type(min_length_comment) == int, "min_length_comment must be int"
  assert type(min_length_OP) == int, "min_length_OP must be int"
  assert type(save_dir) == str, "save_dir must be str"


  max_time_since_OP = round(max_time_since_OP * 3600) # Convert to seconds


  if get_subreddits_sequentially:  

    for i in range(subreddits[0], subreddits[1], 1):

        data_link = link_df.loc[i]['Link']
        subreddit_name = link_df.loc[i]['Name']
        print(f'Working on {subreddit_name}')


        # download and unzip file

        os.makedirs('reddit_data_temp/' + str(i) + "/")
        bashCommand = "wget -P reddit_data_temp/" + str(i) + " " + data_link
        process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)
        zip_dir = "reddit_data_temp/" + str(i)
        zip_file = "reddit_data_temp/" + str(i) + "/" + listdir(zip_dir)[0]
        bashCommand = "unzip -q " + zip_file + " -d reddit_data_temp/" + str(i) 
        process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)
        file = glob.glob(zip_dir + '/*.jsonl')[0]


        # get parents

        with open(file, 'r', encoding='utf-8') as f:
            parents_to_delete = {}
            parents = {}
            id = []
            text = []
            time = []
            scores = []
            for line in f:
                jsonobj = json.loads(line)
                if jsonobj['id'] == jsonobj['root']:
                    if len(jsonobj['text'].split()) >= min_length_OP:
                        current_score = jsonobj['meta']['score']
                        current_time = jsonobj['timestamp']
                        current_id = jsonobj['id']
                        scores.append(current_score)
                        parents_to_delete[current_id] = [current_score, current_time]
                        id.append(current_id)
                        text.append(jsonobj['text'])
                        time.append(current_time)
            corpus = pd.DataFrame(list(zip(id, text, time, scores)), columns=['id', 'text', 'time', 'score'])
            corpus.to_csv(str(subreddit_name) + '_OPs' '.csv', index=False)
            del corpus


            # get top nth parents

            quantile = round(len(scores) / top_OP_denominator)
            scores.sort(reverse=True)
            threshold = scores[quantile]
            for k, v in parents_to_delete.items():
                if v[0] >= threshold:
                    parents[k] = v

            del scores, parents_to_delete



        # get children

        if number_children > 0:
            for i2 in number_children:
                with open(file, 'r', encoding='utf-8') as f:
                    id = []
                    text = []
                    root = []
                    reply = []
                    time = []
                    score = []
                    for line in f:
                        jsonobj = json.loads(line)
                        rt = jsonobj['reply_to']
                        cid = jsonobj['id']
                        if (rt in parents) and (cid not in parents):
                            time_since = jsonobj['timestamp'] - parents[rt][1]
                            if (time_since < max_time_since_OP) and (time_since > 5):
                                txt = jsonobj['text']
                                if (len(txt.split()) >= min_length_comment) and (jsonobj['meta']['stickied'] != 'false'):
                                    id.append(cid)
                                    text.append(str(txt))
                                    root.append(rt)
                                    time.append(time_since)
                                    score.append(int(jsonobj['meta']['score']))
                                    reply.append(str(jsonobj['reply_to']))
                            if len(text) > max_comments_per_subreddit:
                                break

                    corpus = pd.DataFrame(list(zip(id, text, root, time, score, reply)), columns=['id', 'text', 'root', 'time_since_OP', 'upvotes', 'reply_to'])
                corpus.to_csv(save_dir + 'reddit_data_temp/comments_level_' + str(i2) + '.csv', index=False)
                parents = dict(zip(list(corpus.id), [0] * len(corpus.id)))
                del corpus
        


        # concat all datasets

        path = save_dir + 'reddit_data_temp/'
        all_files = glob.glob(os.path.join(path , "*.csv"))

        df_list = []

        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df)

        dff = pd.concat(df_list, axis=0, ignore_index=True)
        dff.to_csv(save_dir + str(subreddit_name) + '.csv', index=False)
        del dff, df_list
        gc.collect()


        # remove temp files from drive

        bashCommand = "rm -r reddit_data_temp/" + str(i) 
        process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)




  else:  

    for i in subreddits:

        data_link = link_df.loc[i]['Link']
        subreddit_name = link_df.loc[i]['Name']
        print(f'Working on {subreddit_name}')


        # download and unzip file

        os.makedirs('reddit_data_temp/' + str(i) + "/")
        bashCommand = "wget -P reddit_data_temp/" + str(i) + " " + data_link
        process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)
        zip_dir = "reddit_data_temp/" + str(i)
        zip_file = "reddit_data_temp/" + str(i) + "/" + listdir(zip_dir)[0]
        bashCommand = "unzip -q " + zip_file + " -d reddit_data_temp/" + str(i) 
        process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)
        file = glob.glob(zip_dir + '/*.jsonl')[0]


        # get parents

        with open(file, 'r', encoding='utf-8') as f:
            parents_to_delete = {}
            parents = {}
            id = []
            text = []
            time = []
            scores = []
            for line in f:
                jsonobj = json.loads(line)
                if jsonobj['id'] == jsonobj['root']:
                    if len(jsonobj['text'].split()) >= min_length_OP:
                        current_score = jsonobj['meta']['score']
                        current_time = jsonobj['timestamp']
                        current_id = jsonobj['id']
                        scores.append(current_score)
                        parents_to_delete[current_id] = [current_score, current_time]
                        id.append(current_id)
                        text.append(jsonobj['text'])
                        time.append(current_time)
            corpus = pd.DataFrame(list(zip(id, text, time, scores)), columns=['id', 'text', 'time', 'score'])
            corpus.to_csv(str(subreddit_name) + '_OPs' '.csv', index=False)
            del corpus


            # get top nth parents

            quantile = round(len(scores) / top_OP_denominator)
            scores.sort(reverse=True)
            threshold = scores[quantile]
            for k, v in parents_to_delete.items():
                if v[0] >= threshold:
                    parents[k] = v
                    
            del scores, parents_to_delete



        # get children

        if number_children > 0:
            for i2 in number_children:
                with open(file, 'r', encoding='utf-8') as f:
                    id = []
                    text = []
                    root = []
                    reply = []
                    time = []
                    score = []
                    for line in f:
                        jsonobj = json.loads(line)
                        rt = jsonobj['reply_to']
                        cid = jsonobj['id']
                        if (rt in parents) and (cid not in parents):
                            time_since = jsonobj['timestamp'] - parents[rt][1]
                            if (time_since < max_time_since_OP) and (time_since > 5):
                                txt = jsonobj['text']
                                if (len(txt.split()) >= min_length_comment) and (jsonobj['meta']['stickied'] != 'false'):
                                    id.append(cid)
                                    text.append(str(txt))
                                    root.append(rt)
                                    time.append(time_since)
                                    score.append(int(jsonobj['meta']['score']))
                                    reply.append(str(jsonobj['reply_to']))
                            if len(text) > max_comments_per_subreddit:
                                break

                    corpus = pd.DataFrame(list(zip(id, text, root, time, score, reply)), columns=['id', 'text', 'root', 'time_since', 'score', 'reply'])
                corpus.to_csv(save_dir + 'reddit_data_temp/comments_level_' + str(i2) + '.csv', index=False)
                parents = dict(zip(list(corpus.id), [0] * len(corpus.id)))
                del corpus
        


        # concat all datasets

        path = save_dir + 'reddit_data_temp/'
        all_files = glob.glob(os.path.join(path , "*.csv"))

        df_list = []

        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df)

        dff = pd.concat(df_list, axis=0, ignore_index=True)
        dff.to_csv(save_dir + str(subreddit_name) + '.csv', index=False)

        del dff, df_list
        gc.collect()


        # remove temp files from drive

        bashCommand = "rm -r reddit_data_temp/" + str(i) 
        process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)