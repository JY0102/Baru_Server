from fastapi import FastAPI , HTTPException , Request , Response
import asyncio

import DB
import make_npy
# uvicorn main:app --host 220.90.180.114:80 --reload
app = FastAPI()
make_npy.convert_all_videos_to_npy()

# 운동 이름 -> npy
@app.get("/get/type/")
async def Get_Npy(exercise):        
    print(exercise)
    
    loop = asyncio.get_running_loop()    
    result = await loop.run_in_executor(None, DB.Get_NpyByName, exercise)                
    
    if not result or len(result) == 0:
        raise HTTPException(status_code=401, detail="Npy Failed")
    
    return Response(content=result, media_type="application/octet-stream")
    
# 회원 가입
@app.post("/user/insert/")
async def Insert_User(id : str , pw : str , name : str):   
    try:
        loop = asyncio.get_running_loop()    
        await loop.run_in_executor(None, DB.Insert_User, id , pw , name)                
        
    
    except HTTPException :
        raise 
    except Exception as e:
        raise HTTPException(status_code=401 , detail= str(e))
    
# 로그인
@app.get("/user/login/")
async def Login_User(id : str , pw : str):
    try:
        
        loop = asyncio.get_running_loop()    
        nickname , strjson =  await loop.run_in_executor(None, DB.Get_LoginUser , id , pw)                
                
        return {"nickname": nickname, "data": strjson}
            
    except HTTPException :
        raise 
    except Exception as e:
        raise HTTPException(status_code= 401 , detail= str(e))
        
# 목표치 저장
@app.post("/data/insert/beforeinfo")
async def Post_BeforeInfo(request: Request):
    try:
        body = await request.json()
        
        baru_id = body.get("baru_id")
        
        goalA = body.get("goalA")
        goalB = body.get("goalB")
        goalC = body.get("goalC")
            
        #Db 저장
        loop = asyncio.get_running_loop()    
        await loop.run_in_executor(None, DB.Insert_BeforeInfo, baru_id , goalA , goalB , goalC)      
    
    except Exception:
        raise HTTPException(status_code=403, detail="Goal Failed")

# 운동 끝난 정보 저장
@app.post("/data/insert/play/")
async def Post_Play(request: Request):
    
    try:
        body = await request.json()
        
        baru_id = body.get("baru_id")
        date = body.get("date")
        name = body.get("name")
        count = int(body.get("count"))
        accuracies = body.get("accuracies")
    
        #DB 저장
        loop = asyncio.get_running_loop()    
        await loop.run_in_executor(None, DB.Insert_Play, baru_id ,date , name , count , accuracies )      
            
    except Exception:
        raise HTTPException(status_code=403, detail="BeforeInfo Failed")
   
@app.get("/data/get")
async def Get_BeforeInfo(baru_id : str):
    loop = asyncio.get_running_loop()    
    result =  await loop.run_in_executor(None, DB.Get_BeforeInfo, baru_id)      
    
    return result
    
    