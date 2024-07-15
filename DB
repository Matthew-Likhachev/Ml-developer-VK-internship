import sqlite3

class DB():
  def __init__(self, name: str = ""):
      if len(name):
        self.t_name = name
        self.create_table(name)

  def create_table(self, name: str = ""):
    self.sqliteConnection = sqlite3.connect(name)
    self.cursor = self.sqliteConnection.cursor()
    self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {name}
            (id string PRIMARY KEY,
            url string,
            pub_date integer,
            fetch_time integer,
            text string, 
            first_fetch_time integer
            )''')
    


  def is_exist(self,id):
    
    self.cursor.execute(f"SELECT * FROM {self.t_name} WHERE id = ?", (id,))
    data=self.cursor.fetchone()
    if data is None:
        #записи не существует
        # print('записи не существует')
        return False
    #запись существует    
    # print('запись существует')
    return True




  def insert(self, data):
    data = tuple([data[0]+str(data[2]),data[0],data[1],data[2],data[3],data[4]])
    if self.is_exist(data[0]):
      return
    # Вставка данных в базу данных
    self.cursor.execute(f"INSERT INTO {self.t_name} (id, url, pub_date, fetch_time, text, first_fetch_time)"
          f" VALUES (?,?,?,?,?,?)",
          data)
    # return True
    
  def commit(self):
    self.sqliteConnection.commit()
  def close(self):
    self.sqliteConnection.close()

  def select_everything(self):
    self.cursor.execute(f"SELECT * FROM {self.t_name}")
    
    print('\nСодержимое БД:')
    for row in self.cursor.fetchall():
        print(row)

