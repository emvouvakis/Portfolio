'''
This project is a Streamlit application that employs EdgeGpt to sent prompts to the Bing AI. 
The purpose is to forecast scores of upcoming football matches.

'''
import streamlit as st
import datetime
import asyncio
from EdgeGPT import Chatbot, ConversationStyle
import pandas as pd
import re 
import numpy as np

st.set_page_config(page_title="Score Prediction", layout="wide")

#Initialize Bot
bot = Chatbot(cookiePath='./cookies.json')

# Define a function to get the chatbot response
async def get_response(prompt):
    response = await bot.ask(prompt=prompt, conversation_style=ConversationStyle.creative)
    msgs = pd.DataFrame(response).loc['messages','item']
    response = pd.DataFrame(msgs).loc[:,'text']
    await bot.close()
    return response.loc[response.index[-1]]

def get_teams(prompt):
    answer = asyncio.run(get_response(prompt))
    ans =  list(answer.split(':')[-1].split('.')[0].split(','))
    stripped_list = [item.strip().split('[')[0] for item in ans]
    return stripped_list

st.title("Bing - Football Score Prediction ⚽")
today = datetime.date.today()
str_date = today.strftime('%Y-%m-%d')
next_days = [(today + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1,10)]

prompt_teams = f'Create a list IN CSV FORMAT DELIMETED WITH | of 3 FOOTBALL teams that are having a match in {next_days}. Use the most popular sources similar to https://www.msn.com/en-us/sports/football, https://www.uefa.com/uefachampionsleague/ so that the football games are not from the past. YOUR RESPONSE MUST BE EXACTLY AS THESE EXAMPLES:  Team1 vs Team2 (24/4) | Team3 vs Team4 (12/2) | Team5 vs Team6 (1/6)'

t = get_teams(prompt_teams)
teams = t[0].split('|')

def get_forecasts(prompt):
    answer2 = asyncio.run(get_response(prompt))
    return answer2

prompt_forecast = f'Make predictions for these 3 football games IN CSV FORMAT DELIMETED WITH | {teams}. Use the most popular sources. Your task is to predict the scores for THE GIVEN football games. Base your predictions on the most important metrics and information you can obtain for the teams. You can include news about injuries, suspensions, lineup changes of the players and metrics about them to improve your predictions. You can also include data from betting sites for your predictions if that improves the accuracy.  A team"s recent form and motivation can also be factors to consider. Use sources similar to http://en.espn.co.uk/football/sport/page/406881.html to identify each team"s squad members (players). YOUR GIVEN ANSWER MUST BE EXACTLY LIKE THESE EXAMPLES: | Teams -> TEAM1 vs TEAM2 (DATE OF GAME), Prediction -> 1-0, Outcome -> TEAM1 | Teams -> TEAM3 vs TEAM4 (DATE OF GAME), Prediction -> 0-1, Outcome -> TEAM4 | Teams -> TEAM5 vs TEAM6 (DATE OF GAME), Prediction -> 0-0, Outcome -> Draw | Reasons for predictions'
answer2 = get_forecasts(prompt_forecast)

ans2, reasoning = answer2.split("Reasons for predictions:")
match_str_list = ans2.split(":")[-1].strip().split("|")

# List comprehension to create a list of dictionaries
match_dicts = [
    {
        "Teams": match_str.split("Prediction")[0].replace("Teams ->", "").replace(",","").strip(),
        "Prediction": match_str.split("Outcome")[0].split("Prediction ->")[-1].replace(",","").strip(),
        "Outcome": match_str.split("Outcome -> ")[-1].strip()
    }
    for match_str in match_str_list
]

df = pd.DataFrame(match_dicts)
# delete cells with less than 5 characters
df = df[df['Teams'].str.len() >= 5]
# Extract the date from the 'Teams' column and create a new 'Date' column
df['Date'] = df['Teams'].str.extract(r'(\d+/\d+)')[0]
# Delete the date from the 'Teams' column
df['Teams'] = df['Teams'].str.replace(r'\s*\(\d+/\d+\)', '')

st.dataframe(df.set_index('Date'), use_container_width=True)

st.write(re.sub(r'\[\^.*?\^\]', '', reasoning))
