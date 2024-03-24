try:
  import unzip_requirements
except ImportError:
  pass

import boto3
import requests

def handler(event, context):
    
    """

    Lambda to track the total clones for your github repos.
    Saves the counts in dynamoDB and sends notification email with SNS.
    After creation, set event trigger for 14 days.
    
    """
    
    github_user = 'USER'
    repos = ['REPO1','REPO2','REPO3']

    token = 'GITHUB_TOKEN'

    headers = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {token}',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    results = {}

    # Send requests to github for all repos
    for repo in repos:
      url = f'https://api.github.com/repos/{github_user}/{repo}/traffic/clones'
      response = requests.get(url, headers=headers)

      if response.status_code == 200:
        data = response.json()
        results[repo] = data['count']

    # Connect to dynamoDB
    dynamodb = boto3.client('dynamodb')

    # Get history
    hist = get_historical_counts(dynamodb)

    # Update dynamoDB
    updated = update_counts(dynamodb, hist, results)

    # Sort the dicts before sending the email
    updated = dict(sorted(updated.items()))
    results = dict(sorted(results.items()))

    # Make email content
    msg = "Last 14D: " + str(results) + '\n'+ '\n' + 'Total: ' + str(updated)

    # Send email
    sender(msg)

    return { 'status':200 }
    

def get_historical_counts(dynamodb):

    # Query dynamoDB
    scan = dynamodb.scan(
        TableName='github-cloning-table'
    )
    items = scan['Items']

    previous_data = {}
    for item in items:
      repo = item['Repo']['S']
      count = int(item['Count']['N'])
      previous_data[repo] = count

    return previous_data

def update_counts(dynamodb, hist:dict, results:dict):
    
    def sum_dicts(dict1, dict2):
      result = {}
      for key in dict1.keys() | dict2.keys():
          result[key] = dict1.get(key, 0) + dict2.get(key, 0)
      return result
   
    updated = sum_dicts(hist, results)
   
    for repo, count in updated.items():
  
      item = {
                  'Repo': {'S': repo},
                  'Count': {'N': str(count)}
              }
      
      dynamodb.put_item(TableName = 'github-cloning-table', Item = item)

    return updated

def sender(msg:str):
	sns = boto3.client('sns')

	sns.publish(
    	TopicArn='TOPIC_ARN',
    	Message=msg,
    	Subject='GithubCloningUpdate'
    )