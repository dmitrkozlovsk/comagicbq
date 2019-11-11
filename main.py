from connector import Connector
from bqhandler import BQTableHanler
from comagichandler import ComagicHandler
from credfile import *

def main(event, context):
    """Функция принимает на вход event, context
    Подбробнее тут:
    https://cloud.google.com/functions/docs/writing/background#functions-writing-background-hello-pubsub-python"""

    #import base64
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    #создает иcточник данных и место назначения данных
    comagic_handler = ComagicHandler(COMAGIC_LOGIN, COMAGIC_PASSWORD, FIRST_CALL_DATE)
    bq_handelr = BQTableHanler(full_table_id, google_credintials_key_path)
    #созздаем коннектор
    connector = Connector(comagic_handler, bq_handelr)
    #обновляем данные в месте назначения
    connector.update_dest_data()

# if __name__ == '__main__':
#     main('test', 'test')
