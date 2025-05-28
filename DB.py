import pandas as pd
from sqlalchemy import create_engine , text
import pyodbc

SERVER = r"DESKTOP-572CNE4"
DATABASE = "WB41"
UID = "aaa"
PWD = "1234"

#데이터베이스 초기 설정
Conn = f"Driver={{SQL Server}};Server={SERVER};Database={DATABASE};UID={UID};PWD={PWD};"

engine = create_engine(r"mssql+pyodbc://aaa:1234@DESKTOP-572CNE4/WB41?driver=ODBC+Driver+17+for+SQL+Server")

TABLE_A = "temp1"
TABLE_B = "temp2"
TABLE_C = "temp3"

# 테이블 생성
with engine.begin() as conn:
    conn.exec_driver_sql(f'''
        IF OBJECT_ID('dbo.{TABLE_A}', 'U') IS NULL
        CREATE TABLE dbo.{TABLE_A} (
        id VARCHAR(8) PRIMARY KEY,
        pw VARCHAR(12),
        name VARCHAR(20)
        )
        ''')
    
    
    conn.exec_driver_sql(f'''
        IF OBJECT_ID('dbo.{TABLE_B}', 'U') IS NULL            
        CREATE TABLE dbo.{TABLE_B} (
        exercise_id INT IDENTITY(1,1) PRIMARY KEY , 
        exercise_name VARCHAR(50)           
        )
        ''')
    
            
    conn.exec_driver_sql(f'''
        IF OBJECT_ID('dbo.{TABLE_C}', 'U') IS NULL
        CREATE TABLE dbo.{TABLE_C} (
        log_id INT IDENTITY(1,1) PRIMARY KEY , 
        id VARCHAR(8),
        exercise_id INT,
        count INT,
        date DateTime,
        
        FOREIGN KEY (id) REFERENCES dbo.{TABLE_A}(id),            
        FOREIGN KEY (exercise_id) REFERENCES dbo.{TABLE_B}(exercise_id)            
        )
        ''')     
 
# 비동기 ( 회원 가입)                                 
def InsertUser(id , pw , name):
    try:                
        sql = f"Insert into {TABLE_A} Values ( ?, ?, ?)"
        params = (id , pw ,name)
    
    
        with pyodbc.connect(Conn) as conn:
            cursor = conn.cursor()
            cursor.execute(sql , params)
            
            cursor.commit()
        
        return True
    except Exception:
        return False
                                
# 비동기 ( 로그인 ) 
# 반환값 -> 유저 닉네임 , 이전 운동기록       
def Get_LoginUser(id , pw):
    print(id , pw)
    sql = f"SELECT name FROM {TABLE_A} WHERE id = ? AND pw = ?"
    params = (id,pw)
    
    with pyodbc.connect(Conn) as conn:
        cursor = conn.cursor()
        cursor.execute(sql , params)

        row = cursor.fetchone()        

    if row:
        sql =f'select date , {TABLE_B}.exercise_name , count from {TABLE_C} Join {TABLE_B} On {TABLE_C}.exercise_id = {TABLE_B}.exercise_id where id = ? order by date Desc'
        params = ( id , )
            
        df = pd.read_sql(sql , con=engine , params= params)    
        json_str =  df.to_json(orient='records')

        return row[0] , json_str
    else:
        return None
        
def Insert_BeforeInfo(id , data : str):
    try:
    
        df = pd.read_json(data)
        
        sql = (f'''
                    BEGIN
                        DECLARE @typeId INT;

                        SELECT 
                            @typeId = exercise_id
                        FROM
                            {TABLE_B}
                        WHERE 
                            exercise_name = :type;

                        INSERT INTO {TABLE_C} VALUES (:id, @typeId, :count, :date);
                    END
                    ''')
        
        with engine.begin() as conn:
            
            for idx , row in df.iterrows():
                
                params = { "type" : row['type'] , "id" : row['id'] , "count" : row['count'] , "date" : row['date'] }
                conn.execute( sql , params)
    except Exception:
        raise
        

    
    