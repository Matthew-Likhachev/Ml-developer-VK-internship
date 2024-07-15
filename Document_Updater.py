from typing import Dict
from DB import DB

class TDocument:
  def __init__(self, url: str, pub_date: int, fetch_time: int, text: str, first_fetch_time: int = None):
      self.url = url
      self.pub_date = pub_date
      self.fetch_time = fetch_time
      self.text = text
      self.first_fetch_time = first_fetch_time


  def __repr__(self):
      return f"TDocument(url={self.url}, pub_date={self.pub_date}, fetch_time={self.fetch_time}, text={self.text}, first_fetch_time={self.first_fetch_time})"

  #Возвращает все параметры класса TDocument в виде словаря
  def get_instance_attributes_dict(self) -> dict:
      res = {}
      for attribute, value in self.__dict__.items():
          res[attribute]= value
      return res

class Processor:
    def __init__(self):
        self.documents: Dict[str, TDocument] = {}


    def process(self, document: TDocument) -> TDocument:
        if not document.url:
            raise ValueError("Document URL cannot be empty")
        if not isinstance(document.url, str):
            raise ValueError("Document URL must be str")
        if not isinstance(document.pub_date, int):
            raise ValueError("Document publish date must be int")
        if not isinstance(document.fetch_time, int):
            raise ValueError("Document fetching time should be int")
        if not isinstance(document.text, str):
            raise ValueError("Document text should be str")
        if (document.first_fetch_time is not None) and (not isinstance(document.first_fetch_time, int)):
            raise ValueError("Document first fetching time should be int or None")
            
        
        existing_doc = self.documents.get(document.url)

        if not existing_doc:
            # Первый документ для данного URL
            document.first_fetch_time = document.fetch_time
            self.documents[document.url] = document
            return document

        # Обновление полей на основе FetchTime
        if document.fetch_time > existing_doc.fetch_time:
            existing_doc.text = document.text
            existing_doc.fetch_time = document.fetch_time
        if document.fetch_time < existing_doc.first_fetch_time:
            existing_doc.first_fetch_time = document.fetch_time
        if document.fetch_time < existing_doc.fetch_time:
            existing_doc.pub_date = document.pub_date

        return existing_doc

#Обработка данных словаря параметров класса TDocument.
def get_save_data(id: int, data: dict) -> list:
  data = [el[1] for el in list(data.items())]
  data.insert(0, id)  #формирование айди записи в БД
  return data

# Пример использования
if __name__ == "__main__":
    processor = Processor()
    db = DB('test', 't1')

    docs = [
        TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1"),
        TDocument(url="doc1", pub_date=90, fetch_time=140, text="Version 2"),
        TDocument(url="doc1", pub_date=110, fetch_time=160, text="Version 3"),
        TDocument(url="doc1", pub_date=110, fetch_time=160, text="Version 3"),
        TDocument(url="doc1", pub_date=70, fetch_time=170, text="Version 4"),


        TDocument(url="doc2", pub_date=90, fetch_time=150, text="Version 1"),
        TDocument(url="doc2", pub_date=70, fetch_time=170, text="Version 2"),
        TDocument(url="doc3", pub_date=110, fetch_time=160, text="Version 1"),
    ]

    for doc in docs:
        updated_doc = processor.process(doc)
        #формирование списка данных для сохранения в БД.
        save = get_save_data(doc.url+str(doc.fetch_time), updated_doc.get_instance_attributes_dict())
        db.insert(save)
        print(f"Processed Document: {updated_doc}")

    #закрытие БД и вывод информации
    db.commit()
    db.select_everything()
    db.close()

