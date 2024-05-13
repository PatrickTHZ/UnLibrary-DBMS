from fastapi import FastAPI
import pandas as pd
import json

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Welcome to Unlibrary"}

@app.get("/author")
async def root():
    df = pd.read_csv("Data_File/author_data.csv")
    json_string =  df.to_json(orient='records')
    return json.loads(json_string)

# Running the app with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)