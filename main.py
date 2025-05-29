from fastapi import FastAPI , HTTPException , Request , Response
import asyncio

import DB

# uvicorn main:app --host 0.0.0.0 --port 6000 --reload
app = FastAPI()


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
    loop = asyncio.get_running_loop()    
    result = await loop.run_in_executor(None, DB.Insert_User, id , pw , name)                
    
    if not result:
        raise HTTPException(status_code=402, detail="Insert Failed")
    
# 로그인
@app.get("/user/login/")
async def Login_User(id : str , pw : str):
    loop = asyncio.get_running_loop()    
    nickname , strjson =  await loop.run_in_executor(None,lambda: DB.Get_LoginUser(id , pw))                
    
    if nickname and strjson:
        return {"nickname": nickname, "data": strjson}
    else:
        raise HTTPException(status_code=402, detail="Login Failed")
        

# 운동 끝난 정보 저장
@app.post("/user/beforeinfo/")
async def Post_BeforeInfo(request: Request):
    
    try:
        body = await request.json()
        
        id = body.get("id")
        accuracies = body.get("Accuracies", [])
    
        #Db 저장
        loop = asyncio.get_running_loop()    
        await loop.run_in_executor(None, DB.Insert_BeforeInfo, id ,accuracies )      
                  
    except Exception:
        raise HTTPException(status_code=403, detail="BeforeInfo Failed")
   
