import time
import requests
import json
import pandas as pd


def get_api_response(counter: str, date1: str, date2: str, token: str, metrics: list, dimensions: list) -> list:
    """
    Получить ответ API Яндекс Метрики
    :param counter: ID счетчика в строковом виде
    :param date1: начальная дата запрашиваемых данных в ISO формате
    :param date2: конечная дата запрашиваемых данных в ISO формате
    :param token: токен доступа к данным счетчика в строковом виде
    :param metrics: список показателей
    :param dimensions: список параметров
    :return: ответ API в виде списка
    """
    try:
        data_content = []
        url = "https://api-metrika.yandex.net/stat/v1/data"
        headers = {"Authorization": "OAuth " + token}
        offset = 1
        repeat_paginator = True
        while repeat_paginator:
            params = {"ids": counter,
                      "metrics": ",".join(metrics),
                      "accuracy": 1,
                      "date1": date1,
                      "date2": date2,
                      "include_undefined": True,
                      "offset": offset,
                      "dimensions": ",".join(dimensions),
                      "limit": 100000}
            request = requests.get(url, headers=headers, params=params)
            response = request.json()
            time.sleep(0.1)
            if response.get("errors"):
                raise Exception(f"{response}")
            data_content = data_content + response.get("data")
            if len(response.get("data")) == 100000:
                offset += 100000
            else:
                repeat_paginator = False
        return data_content
    except Exception as e:
        raise Exception(f"get_api_response error: {e}")


def convert_api_response_to_dataframe(response: list, metrics: list, dimensions: list) -> pd.DataFrame:
    """
    Преобразовать ответ API Яндекс Метрики в объект Pandas DataFrame
    :param response: ответ API Яндекс Метрики в виде списка
    :param metrics: список показателей
    :param dimensions: список параметров
    :return:
    """
    try:
        df = pd.read_json(json.dumps(response))
        for item in dimensions:
            df[item] = df['dimensions'].apply(lambda x: list(x[dimensions.index(item)].values())[0])
        for item in metrics:
            df[item] = df['metrics'].apply(lambda x: x[metrics.index(item)])
        df.drop(columns=['dimensions', 'metrics'], inplace=True)
        return df
    except Exception as e:
        raise Exception(f"convert_api_response_to_dataframe error: {e}")


def save_df_to_file(df: pd.DataFrame, file: str):
    """
    Сохранить Pandas DataFrame в excel файл
    :param df:
    :param file: имя файла без расширения
    :return:
    """
    try:
        if not file.find(".xlsx"):
            raise ValueError("Имя файла не должно содержать расширение .xlsx")

        df.to_excel(f"{file}.xlsx")
    except Exception as e:
        raise Exception(f"convert_api_response_to_dataframe error: {e}")


def main(counter: str = None, date1: str = None, date2: str = None, token: str = None,
         metrics: list = None, dimensions: list = None):
    if not counter:
        counter = "31956481"
    if not date1:
        date1 = "2023-03-01"
    if not date2:
        date2 = "2023-03-05"
    if not token:
        token = "y0_AgAAAAAy83GWAAcdygAAAADbxIQWuSDhDT7MQraOM6qezmcsNLithEY"
    if not metrics:
        metrics = ["ym:s:visits", "ym:s:users", "ym:s:pageviews"]
    if not dimensions:
        dimensions = ["ym:s:date", "ym:s:regionCity",
                      "ym:s:lastsignUTMSource", "ym:s:lastsignUTMMedium", "ym:s:lastsignUTMCampaign"]
    try:
        api_response = get_api_response(counter, date1, date2, token, metrics, dimensions)
        api_df = convert_api_response_to_dataframe(api_response, metrics, dimensions)
        save_df_to_file(api_df, "test")
    except Exception as e:
        raise Exception(f"main error: {e}")


if __name__ == "__main__":
    main()
