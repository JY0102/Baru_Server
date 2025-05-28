from fastapi import FastAPI, UploadFile, File, HTTPException
import httpx
import os
import asyncio

import DB

# uvicorn main:app --reload
app = FastAPI()

ALLOWED_EXTENSIONS = {".npy"}
MAX_FILE_SIZE_MB = 100  # 최대 500MB

#파일 유효성 검사
def validate_file(file: UploadFile):
    # 이름과 확장자를 분할
    _, ext = os.path.splitext(file.filename)
    if ext.lower() not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported file type.")

ACCURACY_API_NAME = "SERVER B"
SERVER_B_URL = "http://localhost:9000/proaess"

async def Get_Accuracy(file: UploadFile):
    async with httpx.AsyncClient() as client:
        
        # Stream 을 그대로 전달
        files = {
            "file": (file.filename, await file.read(), file.content_type)
        }

        try:
            response = await client.post(SERVER_B_URL, files=files)
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"Failed to contact processing server: {e}")

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"{ACCURACY_API_NAME} returned error")

        result = response.json()
        
        return result

# 영상 비교
@app.get("/training/")
async def Get_Training(file: UploadFile = File(...)):        
    try:
        validate_file(file) 

        # 파일 사이즈 제한 검사
        file.file.seek(0, os.SEEK_END)
        size_in_mb = file.file.tell() / (1024 * 1024)
        file.file.seek(0)
        if size_in_mb > MAX_FILE_SIZE_MB:        
            raise HTTPException(status_code=401, detail="File too large.")
        
        result = await Get_Accuracy(file)
    except HTTPException:
        raise

    
# 회원 가입
@app.post("/user/insert/")
async def Insert_User(id : str , pw : str , name : str):   
    loop = asyncio.get_running_loop()    
    result = await loop.run_in_executor(None, DB.InsertUser, id , pw , name)                
    
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
async def Post_BeforeInfo(id : str , data : str):
    try:
        loop = asyncio.get_running_loop()    
        await loop.run_in_executor(None, DB.Insert_BeforeInfo, id , data)                
    except Exception:
        raise HTTPException(status_code=403, detail="BeforeInfo Failed")
   
