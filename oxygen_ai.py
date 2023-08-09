import asyncio

from sqlalchemy import Float, String


def main():
    print('Started AI Process')

    import numpy
    from psycopg2.extensions import register_adapter, AsIs
    from sqlalchemy import Table, Column, Integer, MetaData
    from sqlalchemy.sql.type_api import UserDefinedType
    import json
    from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError
    from sqlalchemy import create_engine, func, text
    from dotenv import load_dotenv
    import os
    import pandas as pd
    from transformers import pipeline
    import keras
    import pickle
    from typing import Union
    import numpy as np

    load_dotenv()

    host = os.getenv("MEMPHIS_HOSTNAME")
    username = os.getenv("MEMPHIS_USERNAME")
    password = os.getenv("MEMPHIS_PASSWORD")
    account_id = os.getenv("MEMPHIS_ACCOUNT_ID")
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')
    pg_host = os.getenv('PG_HOST')
    pg_dbname = os.getenv('PG_DBNAME')

    # hacky solution for numpy64
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)

    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)

    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    conn = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_dbname}"
    )

    classifier = pipeline("sentiment-analysis", model="./blaze_nlp")
    lstm_model = keras.models.load_model("lstm_model.h5")
    scaler = pickle.load(open("scaler.pkl", "rb"))

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

            average_tweet_sentiment = average_tweet_sentiment / len(tweet_batch_result)
            average_tweet_sentiment = round(average_tweet_sentiment)

            temperature_sentiment = derive_sentiment(temperature_batch_result)

            if not average_tweet_sentiment:
                output.append(temperature_sentiment)
            else:
                output.append(average_tweet_sentiment * 0.4 + temperature_sentiment * 0.6)

        return output

    def rolling_samples(start_day, series, window=7):
        output = []
        for i in range(window, len(series)):
            sample = series[i - window: i].tolist()
            output.append((sample, i + start_day - 1))
        return output

    def dict_to_example(sample):
        return sample["tweets"], sample["temperature"]

    def parse_key(s: str):
        day_str, _, coordinates_str = s.split('(')
        day = int(day_str)
        x_str, y_str = coordinates_str.strip(')').strip('(').split(',')
        x = int(x_str)
        y = int(y_str)
        return day, x, y

    async def ai_model(batched_tweets, batched_temperatures, batched_keys):
        memphis = Memphis()
        station_name = "zakar-fire-predictions"
        try:
            await memphis.connect(host=host, username=username, password=password, account_id=account_id)
            producer = await memphis.producer(station_name=f"{station_name}",
                                              producer_name=f"{station_name}-producer")  # you can send the message parameter as dict as well
            for i in range(len(batched_tweets)):
                print(f"Batch {i + 1}/{len(batched_tweets)}")
                predictions = predict_fire(batched_tweets[i], batched_temperatures[i])
                msgs = []
                highest_score = {
                    'score': 0,
                    'day': 0,
                    'x': -1,
                    'y': -1
                }
                lowest_score = {
                    'score': 1,
                    'day': 0,
                    'x': -1,
                    'y': -1
                }
                for j, key in enumerate(batched_keys[i]):
                    day, x, y = parse_key(key)
                    if predictions[j] > 0.7:
                        msg = {
                            "day": day,
                            "geospatial_x": x,
                            "geospatial_y": y,
                        }

                        await producer.produce(bytearray(json.dumps(msg), "utf-8"))

                        msg['score'] = predictions[j]
                        msgs.append(msg)

                    if predictions[j] > highest_score['score']:
                        highest_score['score'] = predictions[j]
                        highest_score['day'] = day
                        highest_score['x'] = x
                        highest_score['y'] = y
                        highest_score['j'] = j
                    elif predictions[j] < lowest_score['score']:
                        lowest_score['score'] = predictions[j]
                        lowest_score['day'] = day
                        lowest_score['x'] = x
                        lowest_score['y'] = y
                        lowest_score['j'] = j

                if len(msgs) > 0:
                    postgres_egress(msgs)

                print(f"Highest score: {highest_score['score']} on day {highest_score['day']} at ({highest_score['x']}, {highest_score['y']})")
                print(f"Lowest score: {lowest_score['score']} on day {lowest_score['day']} at ({lowest_score['x']}, {lowest_score['y']})")

        except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
            print(e)

        finally:
            await memphis.close()

    def postgres_egress(msg_list):
        value_list = []
        insert_statement = ai_firealerts_production.insert()

        for msg in msg_list:
            day = msg['day']
            x = msg['geospatial_x']
            y = msg['geospatial_y']
            score = msg['score']
            id = f"{day}({x},{y})"

            value_list.append({
                'id': id,
                'day': day,
                'xy': func.point(x, y),
                'score': score
            })

        if len(value_list) > 0:
            with conn.connect() as connection:
                print(f"Inserting {len(value_list)} predictions into postgres")
                connection.execute(insert_statement.values(value_list))
                connection.commit()

    metadata = MetaData()

    ai_firealerts_production = Table('ai_fire_alerts_production', metadata,
                                     Column('id', String, primary_key=True),
                                     Column('day', Integer),
                                     Column('xy', Point),
                                     Column('score', Float))

    metadata.create_all(conn)

    print("Temperature starting")
    temp_readings = pd.read_sql_query(text("SELECT * FROM public.temp_readings_production"), conn)

    temp_readings = temp_readings.sort_values("day")

    start_day = temp_readings.iloc[0]["day"]
    print("Temperature done")
    samples = []
    print("Rolling samples")
    for name, group in temp_readings.groupby("xy"):
        group = group.sort_values("day")
        temps = rolling_samples(start_day, group["temperature"].set_axis(group["day"]))

        for temp, i in temps:
            samples.append({
                "day": i,
                "xy": name,
                "temp": temp
            })
    print("Sorting samples")
    samples = pd.DataFrame.from_dict(sorted(samples, key=lambda x: x["day"]))
    print("Sorting done")

    print("Loading tweets")
    tweets = pd.read_sql_query(text("SELECT * FROM public.tweets_production"), conn)

    merged_data = pd.merge(samples, tweets, how='left', on=['xy', 'day']).drop(["id", "score"], axis=1).fillna("")

    complete_samples = merged_data.groupby(['xy', 'day']).agg(
        # include other columns as needed
        tweets=pd.NamedAgg(column='content', aggfunc=list),
        temperature=pd.NamedAgg(column='temp', aggfunc='first')
    ).reset_index()

    complete_samples = complete_samples.sort_values("day")

    complete_samples = complete_samples.to_dict(orient='records')


    # Define batch size
    batch_size = 128

    # print(test_samples[0])
    data = {}
    for sample in complete_samples:
        key = f"{sample['day']}({sample['xy']})"
        items = [item for sublist in map(dict_to_example, [sample]) for item in sublist]
        data[key] = items

    # Split the data into batches
    keys = list(data.keys())
    batched_keys = []
    batched_tweets = []
    batched_temperatures = []
    for i in range(0, len(keys), batch_size):
        batch_keys = keys[i:i + batch_size]
        batch_tweets = [data[key][0] for key in keys[i:i + batch_size]]
        batch_temperatures = [data[key][1] for key in keys[i:i + batch_size]]
        batched_keys.append(batch_keys)
        batched_tweets.append(batch_tweets)
        batched_temperatures.append(batch_temperatures)

    # Call predict_fire on each batch and map the outputs to the keys
    asyncio.run(ai_model(batched_tweets, batched_temperatures, batched_keys))
