#region Import 모음
import pyodbc
import json

from sqlalchemy import create_engine , Column, Integer, String, ForeignKey, JSON , LargeBinary
from sqlalchemy.orm import relationship , joinedload
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
#endregion

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

#region 테이블 생성

Base = declarative_base()

class Exercise(Base):
    __tablename__ = 'Exercise'

    exercise_id = Column(Integer, primary_key=True, autoincrement=True)
    exercise_name = Column(String(50))
    exercise_npy = Column(LargeBinary)
    
class Baru(Base):
    __tablename__ = 'Baru'

    baru_id = Column(String(8), primary_key=True)
    pw = Column(String(12))
    name = Column(String(20))
    
    before_info = relationship("BeforeInfo", back_populates="baru")

class BeforeInfo(Base):
    __tablename__ = 'BeforeInfo'

    Info_id = Column(Integer, primary_key=True, autoincrement=True)
    baru_id = Column(String(8), ForeignKey("Baru.baru_id"))
    goalA = Column(Integer , default=20)
    goalB = Column(Integer , default=20)
    goalC = Column(Integer , default=20)
    
    plays = relationship("Play", back_populates="before_info", cascade="all, delete-orphan")
    baru = relationship("Baru", back_populates="before_info")

class Play(Base):   
    __tablename__ = "Play"

    Play_id = Column(Integer, primary_key=True, autoincrement=True)
    Info_id = Column(Integer, ForeignKey("BeforeInfo.Info_id"))
    Play_date = Column(String)
    
    before_info = relationship("BeforeInfo", back_populates="plays")
    p_data_list = relationship("P_data", back_populates="plays", cascade="all, delete-orphan")

class P_data(Base):
    __tablename__ = "P_data"
    
    P_data_id = Column(Integer, primary_key=True, autoincrement=True)
    Play_id = Column(Integer, ForeignKey("Play.Play_id"))
    P_data_name = Column(String)
    P_data_count = Column(Integer , default= 0)
    P_data_accuracy = Column(JSON)
    
    plays = relationship("Play", back_populates="p_data_list")
    
Base.metadata.create_all(engine)
#endregion

# 세션 생성
Session = sessionmaker(bind=engine)
session = Session()


# 비동기 ( 회원 가입 )                                 
def Insert_User(id , pw , name):
    try:                
        sql = f"Insert into {TABLE_A} Values ( ?, ?, ?)"
        params = (id , pw ,name)
        
        with pyodbc.connect(Conn) as conn:
            cursor = conn.cursor()
            cursor.execute(sql , params)
            
            cursor.commit()
        
        # 목표치 기본값 설정
        Insert_BeforeInfo(id , None , None , None)
        
    except Exception:
        raise Exception('아이디가 이미 존재합니다')
                                
# 비동기 ( 로그인 ) 
# 반환값 -> 유저 닉네임 , 이전 운동기록       
def Get_LoginUser(id , pw):
    sql = f"SELECT name FROM {TABLE_A} WHERE id = ? AND pw = ?"
    params = (id,pw)
    
    row_name = None
    
    with pyodbc.connect(Conn) as conn:
        cursor = conn.cursor()
        cursor.execute(sql , params)

        row_name = cursor.fetchone()        
        if not row_name:
            raise Exception('아이디가 존재하지 않습니다.')
        
    return row_name , Get_BeforeInfo(id)

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
  
# npy파일 , 운동이름 DB에 저장
def Insert_Exercise(name , arr_bytes):
    
    with pyodbc.connect(Conn) as conn:
        sql = f"insert into {TABLE_B} values (? , ?)"
        
        params = (name , arr_bytes)
        
        cursor = conn.cursor()
        cursor.execute(sql , params)
        
        cursor.commit()

# 목표치 재설정 및 추가
def Insert_BeforeInfo(baru_id , goalA , goalB , goalC):
    before_info = session.query(BeforeInfo).filter(BeforeInfo.baru_id == baru_id).first()

    if not before_info:
        baru = session.query(Baru).filter(Baru.baru_id == baru_id).first()
        
        before_info = BeforeInfo()
        before_info.baru = baru  # FK 설정
        # before_info.baru_id = baru.baru_id  # FK 직접 지정
        session.add(before_info)
        
        
    if goalA:
        before_info.goalA = goalA
    if goalB:
        before_info.goalB = goalB
    if goalC:
        before_info.goalC = goalC
        
    session.commit()
        
# 이전정보 저장
def Insert_Play(baru_id : str , date :str , name :str, count : int , accuracy):

    before_info = session.query(BeforeInfo).filter(BeforeInfo.baru_id == baru_id).first()

    # before_info.plays 중에서 해당 날짜 있는지 확인
    play = next((p for p in before_info.plays if p.Play_date == date), None)

    if not play:
        # 없으면 새로 생성 후 before_info에 추가
        play = Play(Play_date=date)
        play.before_info = before_info
        session.add(play)

    pdata = next((pdata for pdata in play.p_data_list if pdata.P_data_name == name), None)

    # P_data 생성
    # 다시
    if not pdata:
        pdata = P_data(P_data_name= name)
        pdata.plays = play
        session.add(play)
        
    pdata.P_data_count = (pdata.P_data_count or 0) + count
    
    if pdata.P_data_accuracy :
        list1 = json.loads(pdata.P_data_accuracy)
        list2 = json.loads(accuracy)

        # 리스트 합치기
        combined_list = list1 + list2

        # 다시 JSON 문자열로 변환
        pdata.P_data_accuracy = json.dumps(combined_list)
    else :        
        pdata.P_data_accuracy = accuracy    


    # 전체 트리를 세션에 추가
    session.add(before_info)  # before_info를 추가하면 연결된 play/p_data까지 다 자동으로 추가됨
    session.commit()
   
# 이전정보 호출    
def Get_BeforeInfo(baru_id):
    
    beforeInfos =( 
        session.query(BeforeInfo) 
        .options(
            joinedload(BeforeInfo.plays)
            .joinedload(Play.p_data_list)
        )
        .filter(BeforeInfo.baru_id == baru_id) 
        .all()
    )
    
    if not beforeInfos:                
        return None

    for beforeInfo in beforeInfos:
        
        result = {
            "목표치A": beforeInfo.goalA,
            "목표치B": beforeInfo.goalB,
            "목표치C": beforeInfo.goalC,
            "운동들": []
        }

        # plays는 날짜별 그룹, 각각 운동이름별로 count와 정확도 리스트를 넣음
        for play in beforeInfo.plays:
            운동_딕셔너리 = {}
            for pdata in play.p_data_list:
                정확도_리스트 = []
                if pdata.P_data_accuracy:
                    try:
                        정확도_리스트 = json.loads(pdata.P_data_accuracy)
                    except Exception:
                        정확도_리스트 = []  # 파싱 실패 시 빈 리스트 처리

                운동_딕셔너리[pdata.P_data_name] = {
                    "총개수": pdata.P_data_count or 0,
                    "정확도": 정확도_리스트
                }
            
            result["운동들"].append({
                "날짜": play.Play_date,
                **운동_딕셔너리
            })

        return result
        