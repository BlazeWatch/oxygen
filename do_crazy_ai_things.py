# from transformers import pipeline
# import keras
# import numpy as np
# import pickle
import numpy
from dotenv import load_dotenv
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, func, select, and_, text
from sqlalchemy.sql.type_api import UserDefinedType
# import os
from tqdm import tqdm
import pandas as pd
#
# from typing import Union

# classifier = pipeline("sentiment-analysis", model="./blaze_nlp")
# lstm_model = keras.models.load_model("lstm_model.h5")
# scaler = pickle.load(open("scaler.pkl", "rb"))
#
# def derive_sentiment(model_output: {"label": str, "score": float}) -> float:
#     """
#     Derives the sentiment score from the model's output.
#
#     :param model_output: A dictionary containing the model's classification label and score.
#     :return: A sentiment score based on the model's output. Positive for 'yes_fire', negative for 'no_fire'.
#     """
#
#     return model_output["score"] * (1 if model_output["label"] == "yes_fire" else -1)
#
#
# def predict_fire_from_temp(temperatures: Union[list[int], list[list[int]]]) -> list[dict]:
#     """
#     Predicts fire based on the given temperatures.
#
#     :param temperatures: A list or list of lists containing temperature values.
#     :return: A list of dictionaries containing the prediction labels ('no_fire' or 'yes_fire') and scores.
#     """
#
#     temperatures = np.array([temperatures]).reshape((-1, 7))
#     temperatures = scaler.transform(temperatures)
#     temperatures = temperatures.reshape(
#         (temperatures.shape[0], temperatures.shape[1], 1)
#     )
#
#     # predict the fire
#     predictions = lstm_model.predict(temperatures, verbose=0)
#     output = []
#     for prediction in list(predictions):
#         prediction_result = np.argmax(prediction)
#         label = ["no_fire", "yes_fire"][prediction_result]
#         score = prediction[prediction_result]
#
#         output.append({"label": label, "score": score})
#
#     return output
#
#
# def predict_fire(
#     tweets: Union[list[str], list[list[str]]],
#     temperatures: Union[list[int], list[list[int]]],
# ) -> list[float]:
#     """
#     Predicts fire based on the given tweets and temperatures.
#
#     :param tweets: A list or list of lists containing tweets. Can be an empty list.
#     :param temperatures: A list or list of lists containing temperature values.
#     :return: A list of sentiment scores combining the information from tweets and temperatures.
#              If tweets is an empty list, the function will return predictions based solely on temperatures.
#     """
#
#     if not isinstance(temperatures[0], list):
#         temperatures = [temperatures]
#
#     if not tweets:
#         # Handle case when tweets is an empty list
#         flat_temperatures = [temp for temp_batch in temperatures for temp in temp_batch]
#         temperature_fire = predict_fire_from_temp(flat_temperatures)
#         return [derive_sentiment(temp) for temp in temperature_fire]
#
#     if len(tweets) and not isinstance(tweets[0], list):
#         tweets = [tweets]
#
#     # Flattening tweets and storing their batch indices
#     flat_tweets = []
#     tweet_batch_indices = [0]
#     for tweet_batch in tweets:
#         flat_tweets.extend(tweet_batch)
#         tweet_batch_indices.append(len(flat_tweets))
#
#     # Flattening temperatures and storing their batch indices
#     flat_temperatures = [temp for temp_batch in temperatures for temp in temp_batch]
#     temperature_batch_indices = [0]
#     for i in range(len(temperatures)):
#         temperature_batch_indices.append(
#             temperature_batch_indices[-1] + len(temperatures[i])
#         )
#
#     # Get predictions for the flattened tweets and temperatures
#     tweet_fire = classifier(flat_tweets)
#     temperature_fire = predict_fire_from_temp(flat_temperatures)
#
#     output = []
#
#     # Process predictions based on indices
#     for i in range(len(temperatures)):
#         tweet_batch_start, tweet_batch_end = (
#             tweet_batch_indices[i],
#             tweet_batch_indices[i + 1],
#         )
#         tweet_batch_result = tweet_fire[tweet_batch_start:tweet_batch_end]
#         temperature_batch_result = temperature_fire[i]
#
#         average_tweet_sentiment = 0
#         for tweet in tweet_batch_result:
#             sentiment = derive_sentiment(tweet)
#             average_tweet_sentiment += sentiment * (0.2 if sentiment < 0 else 1)
#         average_tweet_sentiment = round(average_tweet_sentiment)
#
#         temperature_sentiment = derive_sentiment(temperature_batch_result)
#
#         if not average_tweet_sentiment:
#             output.append(temperature_sentiment)
#         else:
#             output.append(average_tweet_sentiment * 0.4 + temperature_sentiment * 0.6)
#
#     return output
#
# hacky solution for numpy64
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

#
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)
#
# # Load env vars
load_dotenv()

conn = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}/{os.getenv('PG_DBNAME')}"
)
#
#
class Point(UserDefinedType):
    def get_col_spec(self):
        return "POINT"

    def bind_processor(self, dialect):
        def process(value):
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            x, y = map(float, value[6:-1].split())
            return x, y

        return process


metadata = MetaData()

# This code is a mess.
temp_readings_production = Table(
    'temp_readings_production',
    metadata,
    Column('id', Integer, autoincrement=True, primary_key=True),
    Column('day', Integer),
    Column('xy', Point),
    Column('temperature', Integer)
)

tweets_production = Table('tweets_production',
                          metadata,
                          Column('id', Integer, autoincrement=True, primary_key=True),
                          Column('day', Integer), Column('xy', Point), Column('score', Integer),
                          Column('content', String))

firealerts_production = Table('fire_alerts_production', metadata,
                              Column('id', Integer, autoincrement=True, primary_key=True),
                              Column('event_day', Integer), Column('notification_day', Integer),
                              Column('xy', Point))
ai_firealerts_production = Table('ai_fire_alerts_production', metadata,
                                 Column('id', Integer, autoincrement=True, primary_key=True),
                                 Column('event_day', Integer), Column('notification_day', Integer),
                                 Column('xy', Point))

metadata.create_all(conn)


# Select data between day 32 and day 64 from temp_readings_production
# temp_readings_query = select([temp_readings_production]).where(and_(temp_readings_production.c.day >= 32, temp_readings_production.c.day <= 64))
# temp_readings_result = conn.connect().execute(temp_readings_query)
# temp_readings_data = temp_readings_result.fetchall()

# Select data between day 32 and day 64 from tweets_production
# tweets_query = select([tweets_production]).where(and_(tweets_production.c.day >= 32, tweets_production.c.day <= 64))
# tweets_result = conn.connect().execute(tweets_query)
# tweets_data = tweets_result.fetchall()

# temp_readings = pd.read_sql_query(text("SELECT * FROM public.temp_readings_production"), conn)




# def rolling_samples(series, window=7):
#     output = []
#     for i in range(window, len(series)):
#         sample = series[i - window : i].tolist()
#         output.append((sample, i))
#     return output

#
# samples = []
# for name, group in tqdm(temp_readings.groupby("xy")):
#     group = group.sort_values("day")
#     temps = rolling_samples(group["temperature"].set_axis(group["day"]))
#
#     for temp, i in temps:
#         samples.append({
#             "day": i,
#             "xy": name,
#             "temp": temp
#         })
# print("Sorting samples")
# samples = sorted(samples, key=lambda x: x["day"])
# print("Sorting dome")
# with open("samples.pickle", "wb") as f:
#     pickle.dump(samples, f)
#
# print("Loading tweets (dont crash plz)")
# tweets = pd.read_sql_query(text("SELECT * FROM public.tweets_production"), conn)
# with open("tweets.pickle", "wb") as f:
#     pickle.dump(tweets, f)
# print("Sampling tweets")
#
# samples = pickle.load(open("samples.pickle", "rb"))
# tweets = pickle.load(open("tweets.pickle", "rb"))
#
#
#
#
# complete_samples = []
# for sample in tqdm(samples):
#     sampletweets = tweets[(tweets["xy"] == sample["xy"]) & (tweets["day"] == sample["day"])]["content"].tolist()
#     sample["tweets"] = sampletweets if len(sampletweets) != 0 else []
#     complete_samples.append(sample)
#
# with open("complete_samples.pickle", "wb") as f:
#     pickle.dump(complete_samples, f)

# import pandas as pd

# from multiprocessing import Pool, cpu_count
# import pickle
# from tqdm import tqdm

# def process_sample(sample):
#     sampletweets = tweets[(tweets["xy"] == sample["xy"]) & (tweets["day"] == sample["day"])]["content"].tolist()
#     sample["tweets"] = sampletweets if len(sampletweets) != 0 else []
#     return sample

# if __name__ == "__main__":
#     samples =pd.DataFrame.from_dict(pickle.load(open("samples.pickle", "rb")))
#     tweets = pickle.load(open("tweets.pickle", "rb"))
#     tqdm.pandas()
#     # Use the number of available CPUs
#     merged_data = pd.merge(samples, tweets, how='left', on=['xy', 'day'])

#     # Group by 'xy' and 'day', and aggregate the 'content' as a list
#     def aggregate_content(group):
#         group['tweets'] = group['content'].dropna().tolist()
#         return group.iloc[0]

#     complete_samples = merged_data.groupby(['xy', 'day'], group_keys=False).progress_apply(aggregate_content)
#     with open("complete_samples.pickle", "wb") as f:
#         pickle.dump(complete_samples, f)


from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, func, select, and_, text
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm
from transformers import pipeline
import keras
import numpy as np
import pickle
from typing import Union
import numpy as np


def derive_sentiment(model_output: {"label": str, "score": float}) -> float:
    """
    Derives the sentiment score from the model's output.

    :param model_output: A dictionary containing the model's classification label and score.
    :return: A sentiment score based on the model's output. Positive for 'yes_fire', negative for 'no_fire'.
    """

    return model_output["score"] * (1 if model_output["label"] == "yes_fire" else -1)


def predict_fire_from_temp(temperatures: Union[list[int], list[list[int]]]) -> list[dict]:
    """
    Predicts fire based on the given temperatures.

    :param temperatures: A list or list of lists containing temperature values.
    :return: A list of dictionaries containing the prediction labels ('no_fire' or 'yes_fire') and scores.
    """

    temperatures = np.array([temperatures]).reshape((-1, 7))
    temperatures = scaler.transform(temperatures)
    temperatures = temperatures.reshape(
        (temperatures.shape[0], temperatures.shape[1], 1)
    )

    # predict the fire
    predictions = lstm_model.predict(temperatures, verbose=0)
    output = []
    for prediction in list(predictions):
        prediction_result = np.argmax(prediction)
        label = ["no_fire", "yes_fire"][prediction_result]
        score = prediction[prediction_result]

        output.append({"label": label, "score": score})

    return output


def predict_fire(
        tweets: Union[list[str], list[list[str]]],
        temperatures: Union[list[int], list[list[int]]],
) -> list[float]:
    """
    Predicts fire based on the given tweets and temperatures.

    :param tweets: A list or list of lists containing tweets. Can be an empty list.
    :param temperatures: A list or list of lists containing temperature values.
    :return: A list of sentiment scores combining the information from tweets and temperatures.
             If tweets is an empty list, the function will return predictions based solely on temperatures.
    """

    if not isinstance(temperatures[0], list):
        temperatures = [temperatures]

    if not tweets:
        # Handle case when tweets is an empty list
        flat_temperatures = [temp for temp_batch in temperatures for temp in temp_batch]
        temperature_fire = predict_fire_from_temp(flat_temperatures)
        return [derive_sentiment(temp) for temp in temperature_fire]

    if len(tweets) and not isinstance(tweets[0], list):
        tweets = [tweets]

    # Flattening tweets and storing their batch indices
    flat_tweets = []
    tweet_batch_indices = [0]
    for tweet_batch in tweets:
        flat_tweets.extend(tweet_batch)
        tweet_batch_indices.append(len(flat_tweets))

    # Flattening temperatures and storing their batch indices
    flat_temperatures = [temp for temp_batch in temperatures for temp in temp_batch]
    temperature_batch_indices = [0]
    for i in range(len(temperatures)):
        temperature_batch_indices.append(
            temperature_batch_indices[-1] + len(temperatures[i])
        )

    # Get predictions for the flattened tweets and temperatures
    tweet_fire = classifier(flat_tweets)
    temperature_fire = predict_fire_from_temp(flat_temperatures)

    output = []

    # Process predictions based on indices
    for i in range(len(temperatures)):
        tweet_batch_start, tweet_batch_end = (
            tweet_batch_indices[i],
            tweet_batch_indices[i + 1],
        )
        tweet_batch_result = tweet_fire[tweet_batch_start:tweet_batch_end]
        temperature_batch_result = temperature_fire[i]

        average_tweet_sentiment = 0
        for tweet in tweet_batch_result:
            sentiment = derive_sentiment(tweet)
            average_tweet_sentiment += sentiment * (0.2 if sentiment < 0 else 1)
        average_tweet_sentiment = round(average_tweet_sentiment)

        temperature_sentiment = derive_sentiment(temperature_batch_result)

        if not average_tweet_sentiment:
            output.append(temperature_sentiment)
        else:
            output.append(average_tweet_sentiment * 0.4 + temperature_sentiment * 0.6)

    return output

load_dotenv()
conn = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}/{os.getenv('PG_DBNAME')}"
)
def rolling_samples(start_day, series, window=7):
    output = []
    for i in range(window, len(series)):
        sample = series[i - window : i].tolist()
        output.append((sample, i+start_day))
    return output

print("Temp starting")
temp_readings = pd.read_sql_query(text("SELECT * FROM public.temp_readings_production"), conn)

temp_readings = temp_readings.sort_values("day")

start_day = temp_readings.iloc[0]["day"]
print("Temp done")
samples = []
for name, group in tqdm(temp_readings.groupby("xy")):
    group = group.sort_values("day")
    temps = rolling_samples(start_day, group["temperature"].set_axis(group["day"]))
    
    for temp, i in temps:
        samples.append({
            "day": i,
            "xy": name,
            "temp": temp
        })
print("Sorting samples")
samples = sorted(samples, key=lambda x: x["day"])
print("Sorting dome")
# with open("samples.pickle", "wb") as f:
#     pickle.dump(samples, f)

print("Loading tweets (dont crash plz)")
tweets = pd.read_sql_query(text("SELECT * FROM public.tweets_production"), conn)

tqdm.pandas()

merged_data = pd.merge(samples, tweets, how='left', on=['xy', 'day']).drop(["id", "score"], axis=1).fillna("")

complete_samples =merged_data.groupby(['xy', 'day']).agg(
    # include other columns as needed
    tweets=pd.NamedAgg(column='content', aggfunc=list),
    temperature=pd.NamedAgg(column='temp', aggfunc='first')
).reset_index()

complete_samples = complete_samples.to_dict(orient = 'records')

def dict_to_example(sample):
    return sample["tweets"], sample["temperature"]



classifier = pipeline("sentiment-analysis", model="./blaze_nlp")
lstm_model = keras.models.load_model("lstm_model.h5")
scaler = pickle.load(open("scaler.pkl", "rb"))


# Define batch size
batch_size = 128

# print(test_samples[0])
result = {}
for sample in complete_samples:
    key = f"{sample['day']}({sample['xy']})"
    items = [item for sublist in map(dict_to_example, [sample]) for item in sublist]
    result[key] = items


def parse_key(s: str):
    day_str, xy_str = s.split('(')
    day = int(day_str)
    xy = "(" + xy_str.strip(')')  # Include the parentheses in the coordinate string
    return day, xy

data = result
# Split the data into batches
keys = list(data.keys())
batched_tweets = []
batched_temperatures = []
for i in tqdm(range(0, len(keys), batch_size)):
    batch_tweets = [data[key][0] for key in keys[i:i+batch_size]]
    batch_temperatures = [data[key][1] for key in keys[i:i+batch_size]]
    batched_tweets.append(batch_tweets)
    batched_temperatures.append(batch_temperatures)

# Call predict_fire on each batch and map the outputs to the keys
results = {}
for i in tqdm(range(len(batched_tweets))):
    predictions = predict_fire(batched_tweets[i], batched_temperatures[i])
    for j, key in enumerate(keys[i*batch_size : (i+1)*batch_size]):
        results[key] = predictions[j]

for k, v in results.items():

    print(parse_key(k), v)


# with open("tweets.pickle", "wb") as f:
#     pickle.dump(tweets, f)
# print("Sampling tweets")

# samples = pickle.load(open("samples.pickle", "rb"))
# tweets = pickle.load(open("tweets.pickle", "rb"))

