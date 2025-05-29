from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import numpy
import io
import make_npy 

#region 데이터베이스 초기 설정
SERVER = r"DESKTOP-572CNE4"
DATABASE = "WB41"
UID = "aaa"
PWD = "1234"

Conn = f"Driver={{SQL Server}};Server={SERVER};Database={DATABASE};UID={UID};PWD={PWD};"
engine = create_engine(rf"mssql+pyodbc://{UID}:{PWD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")


TABLE_A = "Baru"            # 회원정보 
TABLE_B = "Exercise"        # 운동정보 ( 코드 / 운동 이름 / NPY / NPY 절대경로 ( 테스트 ))
TABLE_C = "BeforeInfo"      # 이전 운동 정보
#endregion

# 테이블 생성
with engine.begin() as conn:
    # TABLE_A
    conn.exec_driver_sql(f'''
        IF OBJECT_ID('dbo.{TABLE_A}', 'U') IS NULL
        CREATE TABLE dbo.{TABLE_A} (
        id VARCHAR(8) PRIMARY KEY,
        pw VARCHAR(12),
        name VARCHAR(20)
        )
        ''')
    
    # TABLE_B
    conn.exec_driver_sql(f'''
        IF OBJECT_ID('dbo.{TABLE_B}', 'U') IS NULL            
        CREATE TABLE dbo.{TABLE_B} (
        exercise_id INT IDENTITY(1,1) PRIMARY KEY , 
        exercise_name VARCHAR(50),
        exercise_npy VARBINARY(MAX),
        )
        ''')
    
    # TABLE_C  
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
def Insert_User(id , pw , name):
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

# id -> 과거 정보
# !! 다시 제작 !!
def Insert_BeforeInfo(id , accuracies : str):
    try:
    
        df = pd.read_json(accuracies)
        
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
        
# 이름 -> Npy 파일
def Get_NpyByName(name):
    sql = f"SELECT exercise_npy FROM {TABLE_B} WHERE exercise_name = ?"
    params = (name ,)

    with pyodbc.connect(Conn) as conn:
        cursor = conn.cursor()
        cursor.execute(sql , params)
    
        row = cursor.fetchone()
        if row is None:
            return None

        npy_bytes = row.exercise_npy  # 또는 row[0]
        
        print('리턴완')
        return npy_bytes
  
def Insert_Exercise(name , arr_bytes):
    
    with pyodbc.connect(Conn) as conn:
        sql = f"insert into {TABLE_B} values (? , ?)"
        
        params = (name , arr_bytes)
        
        cursor = conn.cursor()
        cursor.execute(sql , params)
        
        cursor.commit()


# !! 운동 npy 초기 값 세팅 제작예정 !!
# def Insert_Exercise():
#     names = make_npy.convert_all_videos_to_npy()
    
#     for name in names:
#         arr = numpy.load(name['npy'])
        
#         buffer = io.BytesIO()
#         numpy.save(buffer, arr)
#         buffer.seek(0)
#         arr_bytes = buffer.read()

#         with pyodbc.connect(Conn) as conn:
#             sql = f"insert into {TABLE_B} values (? , ?)"
            
#             params = (name['type'] , arr_bytes)
            
#             cursor = conn.cursor()
#             cursor.execute(sql , params)
            
#             cursor.commit()
        
            
    