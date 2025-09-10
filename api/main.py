from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import sqlglot

import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://app:app@db:5432/demo")

class QueryRequest(BaseModel):
    sql: str

def validate_sql(sql: str):
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
        if parsed.key != "SELECT":
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
