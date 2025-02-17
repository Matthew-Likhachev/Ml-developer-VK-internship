import unittest
import sqlite3
import os
from Document_Updater import Processor, TDocument, get_save_data
from DB import DB



class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.processor = Processor()
        self.db_name = 'test.db'
        self.t_name = 't1'
        self.db = DB(self.db_name, self.t_name)

    def tearDown(self):
        self.db.close()
        os.remove(self.db_name)

    def test_create_table(self):
        self.db.cursor.execute(f"PRAGMA table_info({self.t_name})")
        columns = [description[1] for description in self.db.cursor.fetchall()]
        expected_columns = ['id', 'url', 'pub_date', 'fetch_time', 'text', 'first_fetch_time']
        self.assertEqual(columns, expected_columns)

    def test_insert_document(self):
        doc = TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1")
        save = get_save_data(doc.url + str(doc.fetch_time), doc.get_instance_attributes_dict())
        self.db.insert(save)
        self.db.commit()

        self.db.cursor.execute(f"SELECT * FROM {self.t_name} WHERE id=?", (save[0],))
        row = self.db.cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[1], "doc1")

    def test_get_save_data(self):
        doc = TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1")
        save = get_save_data(doc.url + str(doc.fetch_time), doc.get_instance_attributes_dict())
        self.assertEqual(save, ["doc1"+"150","doc1",100,150,"Version 1", None])

    def test_get_instance_attributes_dict(self):
      doc = TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1")
      attrs = doc.get_instance_attributes_dict()
      real_attrs = {"url":"doc1", "pub_date":100, "fetch_time":150, "text":"Version 1", "first_fetch_time":None}
      self.assertEqual(attrs, real_attrs)

      doc = TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1", first_fetch_time=90)
      attrs = doc.get_instance_attributes_dict()
      real_attrs = {"url":"doc1", "pub_date":100, "fetch_time":150, "text":"Version 1", "first_fetch_time":90}
      self.assertEqual(attrs, real_attrs)

    def test_is_exist(self):
        doc = TDocument(url="doc12", pub_date=100, fetch_time=150, text="Version 1")
        save = get_save_data(doc.url + str(doc.fetch_time), doc.get_instance_attributes_dict())
        self.db.insert(save)
        self.db.commit()

        is_exist = self.db.is_exist(save[0])
        self.assertEqual(is_exist, True)


    def test_processor(self):
        processor = Processor()

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
            save = get_save_data(doc.url + str(doc.fetch_time), updated_doc.get_instance_attributes_dict())
            self.db.insert(save)
            #print(f"Processed Document: {updated_doc}")

        self.db.commit()

        self.db.cursor.execute(f"SELECT * FROM {self.t_name} WHERE url='doc1'")
        rows = self.db.cursor.fetchall()
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[2][3], 160)



    #Тесты на обработку ошибок
    
    def test_empty_url(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="", pub_date=100, fetch_time=150, text="Version 1"))
        self.assertEqual(str(context.exception), "Document URL cannot be empty")

    def test_invalid_url_type(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url=123, pub_date=100, fetch_time=150, text="Version 1"))
        self.assertEqual(str(context.exception), "Document URL must be str")



    def test_empty_pub_date(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date="", fetch_time=150, text="Version 1"))
        self.assertEqual(str(context.exception), "Document publish date cannot be empty")

        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=None, fetch_time=150, text="Version 1"))
        self.assertEqual(str(context.exception), "Document publish date cannot be empty")

    def test_invalid_pub_date_type(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date="invalid", fetch_time=150, text="Version 1"))
        self.assertEqual(str(context.exception), "Document publish date must be int")


    def test_empty_fetch_time(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time="", text="Version 1"))
        self.assertEqual(str(context.exception), "Document fetching time cannot be empty")
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time=None, text="Version 1"))
        self.assertEqual(str(context.exception), "Document fetching time cannot be empty")

    def test_invalid_fetch_time_type(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time="invalid", text="Version 1"))
        self.assertEqual(str(context.exception), "Document fetching time should be int")



    def test_empty_text(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time=150, text=None))
        self.assertEqual(str(context.exception), "Document text cannot be empty")

    def test_invalid_text_type(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time=150, text=123))
        self.assertEqual(str(context.exception), "Document text should be str")
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time=150, text=3.14))
        self.assertEqual(str(context.exception), "Document text should be str")



    def test_empty_first_fetch_time(self):
      with self.assertRaises(ValueError) as context:
          self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1", first_fetch_time=""))
      self.assertEqual(str(context.exception), "Document first fetching time should be int or None")

    def test_invalid_first_fetch_time_type(self):
        with self.assertRaises(ValueError) as context:
            self.processor.process(TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1", first_fetch_time="invalid"))
        self.assertEqual(str(context.exception), "Document first fetching time should be int or None")

    

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
