from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import sqlglot
from openai import OpenAI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import json
from graph import build_graph

import os

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

graph = build_graph()

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://app:app@db:5432/demo")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

class QueryRequest(BaseModel):
    sql: str

def validate_sql(sql: str):
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
        print(parsed)
        print(parsed.key)
        if parsed.key != "select":
            raise ValueError("Only SELECT queries are allowed")
        # אפשר להוסיף בדיקות נוספות: LIMIT, deny-list, וכו'
        return True
    except Exception as e:
        raise ValueError(f"Invalid SQL: {e}")

@app.post("/query")
def run_query(req: QueryRequest):
    try:
        validate_sql(req.sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(req.sql)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB Error: {e}")

    return {"columns": colnames, "rows": rows}

class NLRequest(BaseModel):
    question: str

@app.post("/nl2sql")
def nl2sql(req: NLRequest):
    schema = open("docs/schema_summaries.md").read()

    prompt = f"""
You are a helpful assistant that generates PostgreSQL SELECT queries ONLY.
Return output in JSON with keys: intent, sql.
Schema:
{schema}

User question: {req.question}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    try:
        data = json.loads(resp.choices[0].message.content)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bad LLM output: {e}")

class QueryRequest(BaseModel):
    question: str

@app.post("/ask")
def ask(req: QueryRequest):
    result = graph.invoke({"query": req.question})

    if result.get("action") == "download":
        return FileResponse(result["file_path"], filename="result.csv")
    else:
        response_data = {
            "intent": result.get("intent"),
            "sql": result.get("sql"),
            "rows": result.get("rows"),
            "viz_spec": result.get("viz_spec"),
            "notices": result.get("notices", [])
        }
        
        # אם השאילתה נחסמה, הוסף reasons
        if result.get("blocked") is True or (not result.get("guardrail_ok", True)):
            response_data["blocked"] = True
            # קח קודם מ-state["reasons"], ואם לא קיים – נצלול ל-findings
            reasons = result.get("reasons")
            if not reasons:
                reasons = (result.get("findings") or {}).get("reasons", [])
            response_data["reasons"] = reasons
        
        return JSONResponse(response_data)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}
