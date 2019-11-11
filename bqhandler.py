from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd


class BQTableHanler:
    """Класс для работы с таблицей BigQuery"""
    time_partition_field = 'PARTITION_DATE'
    def __init__(self, full_table_id, service_account_file_key_path = None):
        """Констркутор принимает полное название таблицы в формате
        `myproject.mydataset.mytable`.

        Для аутентификации, если нет Application Default Credentials,
        в качестве дополнительного параметра может принимать путь к файлу
        сервисного аккаунта."""
        self.full_table_id = full_table_id
        project_id, dataset_id, table_id = full_table_id.split(".")
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        if service_account_file_key_path: #если указан путь к сервисному аккаунту
            from google.oauth2 import service_account
            self.credentials = service_account.Credentials.from_service_account_file(
                        service_account_file_key_path,
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],)
            self.bq_client = bigquery.Client(credentials = self.credentials,
                                    project = self.project_id)
        else:
            self.bq_client = bigquery.Client()
        self.dataset = self.bq_client.get_dataset(self.dataset_id)
        self.location = self.dataset.location
        self.table_ref = self.dataset.table(self.table_id)

    def get_last_update(self):
        """Получает дату последнего звонка из таблицы в формате
        Pandas datetime. Если таблицы не существует возвращает False."""
        try:
            self.bq_client.get_table(self.full_table_id)
        except NotFound as error:
            return False
        query = f"""SELECT MAX({self.time_partition_field}) as last_call
                    FROM `{self.full_table_id}`"""
        result = self.bq_client.query(query,location=self.location).to_dataframe()
        date = pd.to_datetime(result.iloc[0,0]).date()
        return date

    def insert_dataframe(self, dataframe):
        """Метод для передачи данных в таблицу BigQuery.
        В качестве параметров принимает Pandas DataFrame.
        Если таблицы не существет, то клиент создаст таблицу и добавит данные."""
        job_config = bigquery.LoadJobConfig() #Определяем секционирование на уровне дней
        job_config._properties['load']['timePartitioning'] = {'type': 'DAY',
                                            'field': self.time_partition_field}

        result = self.bq_client.load_table_from_dataframe(dataframe,
                 self.table_ref, job_config=job_config).result()
        return result
