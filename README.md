# Ml-developer-VK-internship
Задание на стажировку в компанию ВКонтакте лето 2024:
'''
  На вход сервису поступают обновления документов.
  
  message TDocument {
    string Url = 1; // URL документа, его уникальный идентификатор
    uint64 PubDate = 2; // время заявляемой публикации документа
    uint64 FetchTime = 3; // время получения данного обновления документа, может рассматриваться как идентификатор версии. Пара (Url, FetchTime) уникальна.
    string Text = 4; // текст документа
    uint64 FirstFetchTime = 5; // изначально отсутствует, необходимо заполнить
  }
  
  Документы могут поступать в произвольном порядке (не в том, как они обновлялись), также возможно дублирование отдельных сообщений.
  
  Необходимо на выходе формировать такие же сообщения, но с исправленными отдельными полями по следующим правилам (всё нижеуказанное - для группы документов с совпадающим полем Url):
  
   Поля Text и FetchTime должны быть такими, какими были в документе с наибольшим FetchTime, полученным на данный момент.
   Поле PubDate должно быть таким, каким было у сообщения с наименьшим FetchTime.
   Поле FirstFetchTime должно быть равно минимальному значению FetchTime.
  
  Т. е. в каждый момент времени мы берём PubDate и FirstFetchTime от самой первой из полученных на данный момент версий (если отсортировать их по FetchTime), а Text - от самой последней.
  
  Интерфейс в коде можно реализовать таким:
  
  type Processor interface {
  Process(d *Document) (*Document, error)
  }

  Данный код будет работать в сервисе, читающим входные сообщения из очереди сообщений (Kafka или подобное), и записывающем результат также в очередь. Если Process возвращает nil - то в очередь ничего не пишется.
  
  БД можно описать интерфейсами (при необходимости использования) с пояснениями (может примерным sql) для понимания, какая логика скрывается за интерфейсом и почему, любые текстовые пояснения приветствуются.
  
  Код должен быть готов к работе в боевой среде на столько, на сколько возможно (учтено масштабирование, синхронизация и т.п., если такое возможно).
  
  Тесты так же приветствуются.
'''

DB.py:
Было решено создать базу данных с использованием библиотеки sqlite3.
Класс DB обрабатывает ошибки, может подключаться к базе данных и создавать в ней таблицы.
Для удобства также добавил метод, который выводит всё содержимое базы данных.
Формат хранения в базе данных представлен в словаре COLUMN_NAMES:
COLUMN_NAMES = {
 'id': "string PRIMARY KEY",
 'url': 'string',
 'pub_date': 'integer',
 'fetch_time': 'integer',
 'text': 'string', 
 'first_fetch_time': 'integer'
 }

Document_Updater.py:
 Класс TDocument в Document_Updater.py хранит все данные про документ, имеет метод который возвращает словарь параметров (название, значение).
 Класс Processor в Document_Updater.py обрабатывает поступаемые данные, также обрабатывает ошибки по условию.
В Document_Updater.py присутствует также функция перевода из словаря в список для сохранения с добавленным id - get_save_data(id: int, data: dict) -> list:
