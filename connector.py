from helpfunctions import interval_split
import pandas as pd

class Connector:
    """Соединяет источник данных и место назначения данных"""
    time_partition_field = 'PARTITION_DATE' #название поля по-умолчанию. Переопределяет название поля в других классах
    def __init__ (self, source, dest):
        """Констркутор принимает на вход класс источник
        и класс пункта назначения данных"""
        self.source = source
        self.dest = dest
        self.source.time_partition_field = self.time_partition_field
        self.dest.time_partition_field = self.time_partition_field
    def insert_data_in_dest(self, start_date, end_date):
        """Добавляет данные из источника в место назначения.
        В качестве параметров принимает дату старта и дату окончания
        отчеа, которые нужно взять из источника."""
        dates = pd.date_range(start_date, end_date)
        week_intervals = interval_split(dates, 7) #разбиваем данные на периоды по 7 дней
        for week_interval in week_intervals:
            date_from = week_interval[0].strftime("%Y-%m-%d") + " 00:00:00"
            date_till = week_interval[1].strftime("%Y-%m-%d") + " 23:59:59"
            calls_df = self.source.get_calls_report(date_from, date_till)
            self.dest.insert_dataframe(calls_df)
            print (f"Данные с {date_from} по {date_till} добавлены в таблицу")
        return True
    def update_dest_data(self):
        #Получение последней даты звонка из BigQuery
        last_date = self.dest.get_last_update()
        if not last_date: #Если таблицы не существует
            last_date = self.source.day_before_first_call
        yesterday = pd.Timestamp.today(tz='Europe/Moscow').date() - pd.Timedelta(days=1)
        if last_date == yesterday:
            print("Данные уже обновлены")
        else:
            last_date = last_date + pd.Timedelta(days=1)
            self.insert_data_in_dest(last_date, yesterday)
        return True
