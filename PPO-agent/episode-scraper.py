import pandas as pd
import numpy as np
import os
import requests
import json
import datetime
import time

MIN_FINAL_RATING = 1500 # top submission in a match must have reached this score
num_api_calls_today = 0

all_files = []
for root, dirs, files in os.walk('../input/', topdown=False):
    all_files.extend(files)
seen_episodes = [int(f.split('.')[0]) for f in all_files 
                      if '.' in f and f.split('.')[0].isdigit() and f.split('.')[1] == 'json']
print('{} games in existing library'.format(len(seen_episodes)))

NUM_TEAMS = 1
EPISODES = 600 

BUFFER = 1

base_url = "https://www.kaggle.com/requests/EpisodeService/"
get_url = base_url + "GetEpisodeReplay"
list_url = base_url + "ListEpisodes"

# inital team list

r = requests.post(list_url, json = {"teamId":  5586412}) # arbitrary ID, change to leading ID during challenge

rj = r.json()

teams_df = pd.DataFrame(rj['result']['teams'])

teams_df.sort_values('publicLeaderboardRank', inplace = True)
teams_df.head(6)

def getTeamEpisodes(team_id):
    # request
    r = requests.post(list_url, json = {"teamId":  int(team_id)})
    rj = r.json()

    # update teams list
    global teams_df
    teams_df_new = pd.DataFrame(rj['result']['teams'])
    
    if len(teams_df.columns) == len(teams_df_new.columns) and (teams_df.columns == teams_df_new.columns).all():
        teams_df = pd.concat( (teams_df, teams_df_new.loc[[c for c in teams_df_new.index if c not in teams_df.index]] ) )
        teams_df.sort_values('publicLeaderboardRank', inplace = True)
    else:
        print('teams dataframe did not match')
    
    # make df
    team_episodes = pd.DataFrame(rj['result']['episodes'])
    team_episodes['avg_score'] = -1;

    for i in range(len(team_episodes)):
        agents = team_episodes['agents'].loc[i]
        agent_scores = [a['updatedScore'] for a in agents if a['updatedScore'] is not None]
        team_episodes.loc[i, 'submissionId'] = [a['submissionId'] for a in agents if a['submission']['teamId'] == team_id][0]
        team_episodes.loc[i, 'updatedScore'] = [a['updatedScore'] for a in agents if a['submission']['teamId'] == team_id][0]
        
        if len(agent_scores) > 0:
            team_episodes.loc[i, 'avg_score'] = np.mean(agent_scores)

    for sub_id in team_episodes['submissionId'].unique():
        sub_rows = team_episodes[ team_episodes['submissionId'] == sub_id ]
        max_time = max( [r['seconds'] for r in sub_rows['endTime']] )
        final_score = max( [r['updatedScore'] for r_idx, (r_index, r) in enumerate(sub_rows.iterrows())
                                if r['endTime']['seconds'] == max_time] )

        team_episodes.loc[sub_rows.index, 'final_score'] = final_score
        
    team_episodes.sort_values('avg_score', ascending = False, inplace=True)
    return rj, team_episodes

def saveEpisode(epid, rj):
    # request
    re = requests.post(get_url, json = {"EpisodeId": int(epid)})
    
    # save replay
    with open('{}.json'.format(epid), 'w') as f:
        f.write(re.json()['result']['replay'])

    # save episode info
    with open('{}_info.json'.format(epid), 'w') as f:
        json.dump([r for r in rj['result']['episodes'] if r['id']==epid][0], f)

pulled_teams = {}
pulled_episodes = []
start_time = datetime.datetime.now()
r = BUFFER;

while num_api_calls_today < EPISODES:
    # pull team
    top_teams = [i for i in teams_df.id if i not in pulled_teams]
    if len(top_teams) > 0:
        team_id = top_teams[0]
    else:
        break;
        
    # get team data
    team_json, team_df = getTeamEpisodes(team_id); r+=1;
    num_api_calls_today+=1
    print('{} games for {}'.format(len(team_df), teams_df.loc[teams_df.id == team_id].iloc[0].teamName))

    
    team_df = team_df[  (MIN_FINAL_RATING is None or (team_df.final_score > MIN_FINAL_RATING))]
    
    print('   {} in score range from {} submissions'.format(len(team_df), len(team_df.submissionId.unique() ) ) )
    
    team_df = team_df[~team_df.id.isin(pulled_episodes + seen_episodes)]        
    print('      {} remain to be downloaded\n'.format(len(team_df)))
        
    # pull games
    target_team_games = int(np.ceil(EPISODES / NUM_TEAMS))
    if target_team_games + len(pulled_episodes) > EPISODES:
        target_team_games = EPISODES - len(pulled_episodes)
     
    pulled_teams[team_id] = 0
    
    i = 0
    while i < len(team_df) and pulled_teams[team_id] < target_team_games:
        epid = team_df.id.iloc[i]
        if not (epid in pulled_episodes or epid in seen_episodes):
            try:
                saveEpisode(epid, team_json); r+=1;
                num_api_calls_today+=1
            except:
                time.sleep(20)
                i+=1;
                continue;
                
            pulled_episodes.append(epid)
            pulled_teams[team_id] += 1
            try:
                size = os.path.getsize('{}.json'.format(epid)) / 1e6
                print(str(num_api_calls_today) + ': Saved Episode #{} @ {:.1f}MB'.format(epid, size))
            except:
                print('  file {}.json did not seem to save'.format(epid))    
            if r > (datetime.datetime.now() - start_time).seconds:
                time.sleep( r - (datetime.datetime.now() - start_time).seconds)
                

        i+=1;
    print(); print()

