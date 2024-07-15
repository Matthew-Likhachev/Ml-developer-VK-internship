import unittest
import sqlite3
import os
import DB, Document_Updater


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db_name = 'test.db'
        self.t_name = 't1'
        self.db = DB.DB(self.db_name, self.t_name)

    def tearDown(self):
        self.db.close()
        os.remove(self.db_name)

    def test_create_table(self):
        self.db.cursor.execute(f"PRAGMA table_info({self.t_name})")
        columns = [description[1] for description in self.db.cursor.fetchall()]
        expected_columns = ['id', 'url', 'pub_date', 'fetch_time', 'text', 'first_fetch_time']
        self.assertEqual(columns, expected_columns)

    def test_insert_document(self):
        doc = Document_Updater.TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1")
        save = Document_Updater.get_save_data(doc.url + str(doc.fetch_time), doc.get_instance_attributes_dict())
        self.db.insert(save)
        self.db.commit()

        self.db.cursor.execute(f"SELECT * FROM {self.t_name} WHERE id=?", (save[0],))
        row = self.db.cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[1], "doc1")

    def test_processor(self):
        processor = Document_Updater.Processor()

        docs = [
        Document_Updater.TDocument(url="doc1", pub_date=100, fetch_time=150, text="Version 1"),
        Document_Updater.TDocument(url="doc1", pub_date=90, fetch_time=140, text="Version 2"),
        Document_Updater.TDocument(url="doc1", pub_date=110, fetch_time=160, text="Version 3"),
        Document_Updater.TDocument(url="doc1", pub_date=110, fetch_time=160, text="Version 3"),
        Document_Updater.TDocument(url="doc1", pub_date=70, fetch_time=170, text="Version 4"),
        Document_Updater.TDocument(url="doc2", pub_date=90, fetch_time=150, text="Version 1"),
        Document_Updater.TDocument(url="doc2", pub_date=70, fetch_time=170, text="Version 2"),
        Document_Updater.TDocument(url="doc3", pub_date=110, fetch_time=160, text="Version 1"),
        ]

        for doc in docs:
            updated_doc = processor.process(doc)
            save = Document_Updater.get_save_data(doc.url + str(doc.fetch_time), updated_doc.get_instance_attributes_dict())
            self.db.insert(save)
            print(f"Processed Document: {updated_doc}")

        self.db.commit()

        self.db.cursor.execute(f"SELECT * FROM {self.t_name} WHERE url='doc1'")
        rows = self.db.cursor.fetchall()
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[2][3], 160)

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
