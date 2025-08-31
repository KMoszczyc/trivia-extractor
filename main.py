import pandas as pd
import requests
import html
import copy
import random
import time

OUTPUT_PATH = 'trivia.parquet'

api_url = "https://opentdb.com/api.php?amount=50&token="
get_token_api = "https://opentdb.com/api_token.php?command=request"

letters = ['a', 'b', 'c', 'd']
columns = ['type', 'difficulty', 'category', 'question', 'answers', 'solution', 'correct_answer']

def run():
    merged_df = pd.DataFrame()
    token = get_token() # get session token, once we get all the questions it will give a different response code
    response_code, data = get_data(token)

    count = 0
    while response_code == 0:
        new_df = transform_data(data)
        merged_df = pd.concat([merged_df, new_df], ignore_index=True)

        time.sleep(5)
        response_code, data = get_data(token)
        count += 50
        print(count, response_code)

    print(merged_df.describe())
    print(merged_df.head(5))
    merged_df.to_parquet(OUTPUT_PATH)


def get_token():
    response = requests.get(get_token_api)
    return response.json()['token']

def get_data(token):
    full_api_url = api_url + token
    response = requests.get(full_api_url)

    if response.status_code != 200:
        return response.status_code, None

    response_json = response.json()
    data = response_json['results']
    response_code = response_json['response_code']
    escaped_data = decode_html_entities(data)

    return response_code, escaped_data

def transform_data(data):
    transformed_data = []
    for entry in data:
        new_entry = copy.deepcopy(entry)
        if entry['type'] == 'boolean':
            new_entry['answers'] = ['True', 'False']
        elif entry['type'] == 'multiple':
            new_entry['answers'] = entry['incorrect_answers'] + [entry['correct_answer']]

        random.shuffle(new_entry['answers'])
        answer_index = get_correct_answer_index(new_entry['answers'], entry['correct_answer'])
        new_entry['answers'] = add_abcd(new_entry['answers'])
        new_entry['solution'] = letters[answer_index]

        new_row = [new_entry['type'], new_entry['difficulty'], new_entry['category'], new_entry['question'], new_entry['answers'], new_entry['solution'], new_entry['correct_answer']]
        transformed_data.append(new_row)

    df = pd.DataFrame(transformed_data, columns=columns)
    return df

def get_correct_answer_index(answers, correct_answer):
    return answers.index(correct_answer)

def add_abcd(answers):
    for i, answer in enumerate(answers):
        answers[i] = f'{letters[i]}) {answer}'
    return answers

def decode_html_entities(obj):
    if isinstance(obj, dict):
        return {k: decode_html_entities(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_html_entities(i) for i in obj]
    elif isinstance(obj, str):
        return html.unescape(obj)
    else:
        return obj

if __name__ == '__main__':
    run()
