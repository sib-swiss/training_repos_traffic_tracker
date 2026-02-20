import os
import sys
from datetime import timedelta, datetime

from github import Github, Auth

def write_table( data , repo_list , file = sys.stdout ):
    """writes view/clone data as a csv file
    data is expected to be a dictionary
    whose keys are datetime
    and values are dictionaries
        whose keys are repo name
        and values are corresponding value (eg, number of views for a given date for a given repo)
    """

    print( "date" , *repo_list , sep = ',' , file=file )
    for d in data:
        print( d , *[data[d][r] for r in repo_list ] , sep = ',' , file=file )
    
def read_table( file ):
    """
    reads a csv file and returns it as a dictionary
    whose keys are datetime
    and values are dictionaries
        whose keys are repo name
        and values are corresponding value (eg, number of views for a given date for a given repo)
    """
    header = file.readline().strip()
    repo_list = header.split(',')[1:]
    
    data = {}
    
    for l in file:
        sl = l.strip().split(',')
        date = datetime.fromisoformat( sl[0] )
        data[ date ] = {}
        
        for i,count in enumerate( sl[1:] ):
            data[ date ][ repo_list[i] ] = count
    
    return data

def get_metrics( repo , github_object , raise_error = True):
    """ given 1 repository name and a github object, gather views and clones data.

    returns the data in a dictionary whose keys are 'view_count','view_unique','clone_count', or 'clone_unique'
    and whose values are dictionaries whose keys are datetime and value are the corresponding value (ie, number of views, or clone,..)

    If the raise_error argument is False, if the code fails to gather the repo data (likely because it lacks authorization) it returns the expected object without data
    Otherwise the thrown error is raised
    """
    repo = github_object.get_repo( repo )
    
    data = {}
    data['view_count'] = {}
    data['view_unique'] = {}
    data['clone_count'] = {}
    data['clone_unique'] = {}

    try:
        views = repo.get_views_traffic()
        for v in views.views:
            data['view_count'][v.timestamp]  = v.count
            data['view_unique'][v.timestamp] = v.uniques

        clones = repo.get_clones_traffic()
        for c in clones.clones:
            data['clone_count'][c.timestamp]  = c.count
            data['clone_unique'][c.timestamp] = c.uniques
    except Exception as e:
        if raise_error:
            raise e

    return data

def get_referrers_and_paths( repo , github_object , raise_error = True):
    """ given 1 repository name and a github object, gather top referrers and popular paths.

    returns the data in a dictionary with keys 'referrers' and 'paths'
    each containing a list of dictionaries with the aggregated data over the last 14 days

    If the raise_error argument is False, if the code fails to gather the repo data (likely because it lacks authorization) it returns empty lists
    Otherwise the thrown error is raised
    """
    repo = github_object.get_repo( repo )
    
    data = {}
    data['referrers'] = []
    data['paths'] = []

    try:
        # Get top 10 referral sources (aggregated over last 14 days)
        referrers = repo.get_top_referrers()
        for r in referrers:
            data['referrers'].append({
                'referrer': r.referrer,
                'count': r.count,
                'uniques': r.uniques
            })

        # Get top 10 popular paths (aggregated over last 14 days)
        paths = repo.get_top_paths()
        for p in paths:
            data['paths'].append({
                'path': p.path,
                'title': p.title,
                'count': p.count,
                'uniques': p.uniques
            })
    except Exception as e:
        if raise_error:
            raise e

    return data

def complement_data_structure( min_date,  max_date, repo_list, data = {} ):
    """creates or complement data structure to store 1 metric
    data is expected to be a dictionary
    whose keys are datetime
    and values are dictionaries
        whose keys are repo name
        and values are corresponding value (eg, number of views for a given date for a given repo)
    """
    
    if len(data)>0: ## there is already data 
        # -> update minimum date
        min_date = min(min_date , *(data.keys()) )
        # -> update maximum date
        max_date = max(max_date , *(data.keys()) )
        
        # -> add repos data if they are absent. initialize their value to NA
        for k in data:
            for r in repo_list:
                if not r in data[k]:
                    data[k][r] = "NA"
    
    ## making sure we have data for all days. initializing all at 0
    delta = timedelta(days=1)
    current = min_date
    while current <= max_date:
        if not current in data: # date absent from the current data -> initialize all at 0
            data[current] = { r:0 for r in repo_list }
        current += delta
        
    return data


# using an access token
auth = Auth.Token(os.environ["TRAFFIC_ACTION_TOKEN"])

# Public Web Github
g = Github(auth=auth)


repo_list_file = sys.argv[1]


## files for view_count, view_unique, clone_count, clone_unique
files = { 'view_count' : sys.argv[2],
          'view_unique' : sys.argv[3],
          'clone_count' : sys.argv[4],
          'clone_unique' : sys.argv[5]}

## files for referrers and paths (if provided)
referrers_file = sys.argv[6] if len(sys.argv) > 6 else None
paths_file = sys.argv[7] if len(sys.argv) > 7 else None

## reading the repo list
repo_list = []
with open(repo_list_file) as IN:
    for l in IN:
        repo_list.append( l.strip() )



## reading the data we already have
pre_data = {}
for k in files:
    pre_data[k] = {}
    if os.path.exists(files[k]):
        with open(files[k]) as IN:
            pre_data[k] = read_table(IN)


## gathering the data from github
raw_data = {}
referrers_data = {}
paths_data = {}
for r in repo_list:
    raw_data[r] = get_metrics( repo = r, 
                               github_object = g,
                               raise_error = False )
    
    # Get referrers and paths if output files are specified
    if referrers_file or paths_file:
        ref_path_data = get_referrers_and_paths( repo = r,
                                                 github_object = g,
                                                 raise_error = False )
        referrers_data[r] = ref_path_data['referrers']
        paths_data[r] = ref_path_data['paths']


## determining the time window of the data
all_dates = set()
for r,data in raw_data.items():
    for data2 in data.values():
        all_dates.update( data2.keys() )
min_date , max_date = min(all_dates) , max(all_dates)

## adding the new dates to the data structure
data = {}

for k in pre_data:
    data[k] = complement_data_structure( min_date,  max_date, 
                                        repo_list=repo_list, 
                                        data = pre_data[k] )


## adding the new data 
for k in pre_data:
    for repo in raw_data:
        
        for date,value in raw_data[repo][k].items():
            data[k][date][repo] = value


# ensuring folders containing the output files are created
for k in data:
    folder = k.rpartition("/")[0]
    if folder != '' and folder != '.':
        if not os.path.exists(folder):
            os.makedirs(folder)

## writing data to files
for k in data:
    with open( files[k],'w') as OUT:
        write_table( data[k] , repo_list , file = OUT )

## writing referrers and paths data (snapshot with current timestamp)
import json

if referrers_file and referrers_data:
    current_time = datetime.now()
    
    # Read existing data if file exists
    existing_referrers = []
    if os.path.exists(referrers_file):
        with open(referrers_file, 'r') as IN:
            try:
                existing_referrers = json.load(IN)
            except:
                existing_referrers = []
    
    # Append new snapshot
    snapshot = {
        'timestamp': current_time.isoformat(),
        'data': referrers_data
    }
    existing_referrers.append(snapshot)
    
    # Ensure folder exists
    folder = referrers_file.rpartition("/")[0]
    if folder != '' and folder != '.':
        if not os.path.exists(folder):
            os.makedirs(folder)
    
    # Write to file
    with open(referrers_file, 'w') as OUT:
        json.dump(existing_referrers, OUT, indent=2)

if paths_file and paths_data:
    current_time = datetime.now()
    
    # Read existing data if file exists
    existing_paths = []
    if os.path.exists(paths_file):
        with open(paths_file, 'r') as IN:
            try:
                existing_paths = json.load(IN)
            except:
                existing_paths = []
    
    # Append new snapshot
    snapshot = {
        'timestamp': current_time.isoformat(),
        'data': paths_data
    }
    existing_paths.append(snapshot)
    
    # Ensure folder exists
    folder = paths_file.rpartition("/")[0]
    if folder != '' and folder != '.':
        if not os.path.exists(folder):
            os.makedirs(folder)
    
    # Write to file
    with open(paths_file, 'w') as OUT:
        json.dump(existing_paths, OUT, indent=2)

