import json
import requests
import random
import pandas as pd


class ComagicClient:
    """Базовый класс для работы с CoMagic"""
    def __init__(self, login, password):
        """Конструктор принимает логин и пароль CoMagic"""
        self.login = login
        self.password = password
        self.base_url = 'https://dataapi.comagic.ru/v2.0'
        self.payload_ = {"jsonrpc":"2.0",
                         "id":1,
                         "method":None,
                         "params": None}
        self.token = self.get_token(self.login, self.password)
    def base_request(self, method, params):
        """Основной метод для запросов в CoMagic. В качестве параметра принимает метод API
        и параметры запроса. Возвращает ответ от в JSON-like формате.
        Подбробнее: https://www.comagic.ru/support/api/data-api/"""
        id_ = random.randrange(10**7) #случайный идентификатор
        payload = self.payload_.copy()
        payload["method"] = method
        payload["params"] = params
        payload["id"] = id_
        self.r = requests.post(self.base_url, data=json.dumps(payload))
        self.last_response = json.loads(self.r.text)
        return self.last_response
    def get_token(self, login, password):
        """Метод для получения токена. В качестве параметров принимает
        логин и пароль. Возвращает токен."""
        method = "login.user"
        params = {"login":self.login,
                  "password":self.password}
        response = self.base_request(method, params)
        token = response['result']['data']['access_token']
        return token
    def get_report_per_page(self, method, params):
        """Реализация постраничного вывода. Отправляет запрос и получает
        по 10000 записей. Есть ограничение на вывод.
        Не может получить более 110000 записей.
        Возвращает ответ от в JSON-like формате."""
        response = self.base_request(method, params)
        print(f"""Запрос звонков c {params["date_from"]} до {params["date_till"]}. Сдвиг = {params["offset"]}""")
        result = response['result']['data']
        if len(result) < 10000:
            return result
        else:
            params['offset'] += 10000
            add_result = self.get_report_per_page(method, params)
            return result + add_result
    def get_basic_report(self, method, fields, date_from, date_till, filter=None, offset=0):
        """Метод для получения отчетов.
        Вид отчета и поля определются параметрами method и fields.
        Возвращает список словарей с записями по звонкам.
        В качестве параметров принимает время начала периода,
        время конца периода, параметры звонка и сдвиг для постраничного вывода.

        method -- <string> метод отчета
        date_from -- <string> дата старта. Формат "YYYY-MM-DD hh:mm:ss"
        date_till -- <string> дата окончания. Формат "YYYY-MM-DD hh:mm:ss"
        fields -- <list>, представление возвращаемых данных
        filter [ОПЦИОНАЛЬНО] - <dict> фильтр
        offset [ОПЦИОНАЛЬНО] -- <int> сдвиг

        return -- <list> отчет
        """
        params = {"access_token":self.token,
                  "limit":10000,
                  "date_from":date_from,
                  "date_till":date_till,
                  "fields": fields,
                  "offset": offset}
        if filter:
            params['filter'] = filter
        report = self.get_report_per_page(method, params)
        return report

class ComagicHandler(ComagicClient):
    """Класса для работы с данными, получаемыми от CoMagic"""
    time_partition_field = 'PARTITION_DATE'
    def __init__(self, login, password, first_call_date):
        self.day_before_first_call = pd.to_datetime(first_call_date) - pd.Timedelta(days=1)
        super().__init__(login, password)
    def get_calls_report(self, date_from, date_till):
        """Получение отчета по звонкам с дополнтиельными параметрами за период.
        В качетве параметров принимает дату начала отчета и дату завершения
        отчета.

        Преобразовывает данные в Pandas DataFrame.
        Создает колонку для партицирования по дням. Название колонки полчает из
        класса Connector или берет по умолчанию.
        Преобразовывает колонку с тегами. Оставляет только название тегов.

        Возвращет Pnadas.DataFrame"""
        method = "get.calls_report"
        fields = ['id', 'visitor_id', 'person_id',
                 'start_time', 'finish_reason', 'is_lost', 'tags',
                 'campaign_name','communication_number', 'contact_phone_number',
                 'talk_duration', 'clean_talk_duration', 'virtual_phone_number',
                 'ua_client_id', 'ym_client_id', 'entrance_page',
                 'gclid', 'yclid', 'visitor_type', 'visits_count',
                 'visitor_first_campaign_name', 'visitor_device',
                 'site_domain_name','utm_source', 'utm_medium', 'utm_campaign',
                 'utm_content', 'eq_utm_source', 'eq_utm_medium',
                 'eq_utm_campaign', 'attributes']


        #Получение данных из CoMagic
        calls_data = self.get_basic_report(method, fields, date_from, date_till)
        #Создание DataFrame
        df = pd.DataFrame(calls_data)
        #Создание поля с датой звонка. Поле используется для партицирования.
        df[self.time_partition_field] = pd.to_datetime(df.start_time).apply(lambda x: x.date())
        #Преобразование поле tags, так как BigQuery не может работать с таким типом данных, который
        #отдает CoMagic. Оставляем только название тэгов.
        df['tags'] = df.tags.apply(lambda x: x if x == None else [i['tag_name'] for i in x])
        return df
