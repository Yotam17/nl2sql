from langgraph.graph import StateGraph, END
from utils import save_to_csv
from openai import OpenAI
from guardrails import apply_guardrails
import psycopg2
import os
import json
from datetime import date, datetime
from decimal import Decimal

# מצב משותף (State)
class GraphState(dict):
    query: str
    intent: str
    sql: str
    rows: list
    viz_spec: dict
    action: str   # "display" | "download"
    file_path: str
    notices: list  # הודעות על שינויים שנעשו
    reasons: list  # סיבות על חסימה
    guardrail_ok: bool  # האם השאילתה עברה את ה-guardrails

# יצירת OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def convert_to_json_serializable(obj):
    """ממיר אובייקטים לא JSON-serializable לפורמט JSON"""
    if isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    else:
        return obj

# ---------- Nodes ----------

def detect_intent(state: GraphState):
    query = state["query"]
    
    prompt = f"""
אתה עוזר AI שמזהה את הכוונה של שאילתות משתמשים במערכת ניהול נתונים.

הזהה את הכוונה של השאילתה הבאה וחזור רק עם אחת מהאפשרויות הבאות:
- "sql" - אם המשתמש רוצה לשאול שאילתה SQL או לקבל נתונים טבלאיים
- "viz" - אם המשתמש רוצה לראות גרף, תרשים, או ויזואליזציה של הנתונים

דוגמאות:
- "תראה לי את כל הלקוחות" → sql
- "כמה הזמנות יש לנו השבוע?" → sql  
- "תעשה לי גרף של המכירות לפי חודש" → viz
- "תרשים של התפלגות הגילאים" → viz
- "איזה מוצרים הכי נמכרים?" → sql
- "תציג לי תרשים עוגה של קטגוריות" → viz

שאילתת המשתמש: {query}

הכוונה:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=10
        )
        
        intent = response.choices[0].message.content.strip().lower()
        
        # וידוא שהתשובה תקינה
        if intent in ["sql", "viz"]:
            state["intent"] = intent
        else:
            # fallback ללוגיקה פשוטה אם התשובה לא מוכרת
            q = query.lower()
            if "chart" in q or "plot" in q or "graph" in q or "תרשים" in q or "גרף" in q:
                state["intent"] = "viz"
            else:
                state["intent"] = "sql"
                
    except Exception as e:
        print(f"Error in detect_intent: {e}")
        # fallback ללוגיקה פשוטה במקרה של שגיאה
        q = query.lower()
        if "chart" in q or "plot" in q or "graph" in q or "תרשים" in q or "גרף" in q:
            state["intent"] = "viz"
        else:
            state["intent"] = "sql"
    
    return state

def generate_sql(state: GraphState):
    query = state["query"]
    notices = state.get("notices", [])
    
    # קריאת ה-schema
    try:
        schema = open("docs/schema_summaries.md").read()
    except FileNotFoundError:
        schema = """
        customers(id, name, country)
        orders(id, customer_id, order_date, total_amount)
        items(id, order_id, sku, product_name, qty, unit_price)
        """
    
    prompt = f"""
אתה עוזר AI שיוצר שאילתות PostgreSQL SELECT בלבד.

השתמש בסכמה הבאה:
{schema}

הנחיות:
1. צור רק שאילתות SELECT
2. השתמש בשמות הטבלאות והעמודות המדויקים מהסכמה
3. הוסף LIMIT אם לא צוין אחרת
4. השתמש ב-JOINs כשצריך לקשר בין טבלאות
5. השתמש ב-ORDER BY ו-GROUP BY כשמתאים
6. חזור רק עם השאילתה SQL, ללא הסברים, ללא JSON, ללא backticks
7. תיצור SQL תקין
8.  כשאתה בודק ערכים של מחרוזות (כמו שמות מדינות, מוצרים, שמות לקוחות) – 
   עטוף אותם תמיד במרכאות יחידות ('...').

דוגמאות:
- "תראה לי לקוחות" → SELECT * FROM customers LIMIT 10;
- "הזמנות של לקוח 1" → SELECT * FROM orders WHERE customer_id = 1 LIMIT 10;
- "תן לי את כל הלקוחות לא מישראל" → SELECT * FROM customers WHERE country != 'Israel' LIMIT 10;


שאילתת המשתמש: {query}

החזר רק את השאילתה SQL:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=200
        )
        
        sql = response.choices[0].message.content.strip()
        
        # ניקוי השאילתה - הסרת backticks אם יש
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        
        sql = sql.strip()
        
        # הסרת JSON אם ה-LLM החזיר JSON במקום SQL
        if sql.startswith("{") and sql.endswith("}"):
            try:
                import json
                json_data = json.loads(sql)
                sql = json_data.get("sql", sql)
            except:
                pass
        
        # וידוא שהשאילתה מסתיימת ב-;
        if not sql.endswith(";"):
            sql += ";"
        
        # בדיקה אם יש LIMIT, אם לא - הוספה
        sql_upper = sql.upper()
        if "LIMIT" not in sql_upper:
            sql = sql.rstrip().rstrip(';')
            sql += " LIMIT 10;"
            notices.append("Added LIMIT 10 for safety.")
            
        state["sql"] = sql
        state["notices"] = notices
        
    except Exception as e:
        print(f"Error in generate_sql: {e}")
        # fallback לשאילתה פשוטה במקרה של שגיאה
        q = query.lower()
        if "orders" in q:
            state["sql"] = "SELECT * FROM orders LIMIT 5;"
        else:
            state["sql"] = "SELECT * FROM customers LIMIT 5;"
        state["notices"] = notices
    
    return state

def apply_sql_guardrails(state: GraphState):
    """מחיל guardrails על השאילתה"""
    sql = state["sql"]
    notices = state.get("notices", [])
    
    try:
        # חיבור למסד הנתונים
        DATABASE_URL = os.getenv("DATABASE_URL", "postgres://app:app@db:5432/demo")
        conn = psycopg2.connect(DATABASE_URL)
        
        # החלת guardrails
        result = apply_guardrails(conn, sql)
        state["sql"] = result["sql"]
        state["notices"] = state.get("notices", []) + result.get("notices", [])
        state["findings"] = result.get("findings", {})
        state["reasons"] = result.get("reasons", [])   # <-- תמיד נשמר, גם אם ריק
        state["guardrail_ok"] = result["ok"]
        state["blocked"] = not result["ok"]
                
        # אם השאילתה נחסמה
        if not result["ok"]:
            state["rows"] = []
            state["findings"] = result["findings"]  # העברת findings ל-state
            # חשיפה ישירה של הסיבות לחסימה
            # לא מוסיפים notice על חסימה - זה יועבר דרך reason
            conn.close()
            return state
        
        conn.close()
        
    except Exception as e:
        print(f"Error in apply_sql_guardrails: {e}")
        notices.append(f"Guardrail check failed: {str(e)}")
        notices.append("Skipping guardrails and proceeding with query execution")
        state["notices"] = notices
        state["guardrail_ok"] = True  # ממשיך למרות השגיאה
    
    return state

def execute_sql(state: GraphState):
    """מריץ את השאילתה על מסד הנתונים"""
    sql = state["sql"]
    notices = state.get("notices", [])
    
    try:
        # חיבור למסד הנתונים
        DATABASE_URL = os.getenv("DATABASE_URL", "postgres://app:app@db:5432/demo")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # הרצת השאילתה
        cur.execute(sql)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        
        # המרה לרשימת dictionaries עם המרת JSON
        state["rows"] = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                colname = colnames[i]
                row_dict[colname] = convert_to_json_serializable(value)
            state["rows"].append(row_dict)
        
        cur.close()
        conn.close()
        
        
        
    except Exception as e:
        print(f"Error in execute_sql: {e}")
        notices.append(f"SQL execution failed: {str(e)}")
        notices.append("Using mock data instead")
        state["notices"] = notices
        
        # Fallback לנתונים mock
        if "customers" in sql.lower():
            state["rows"] = [
                {"id": 1, "name": "Alice", "country": "United States"},
                {"id": 2, "name": "Bob", "country": "United Kingdom"},
                {"id": 3, "name": "Charlie", "country": "Germany"},
            ]
        elif "orders" in sql.lower():
            state["rows"] = [
                {"id": 1, "customer_id": 1, "order_date": "2024-01-15", "total_amount": 120.50},
                {"id": 2, "customer_id": 2, "order_date": "2024-01-16", "total_amount": 89.99},
            ]
        else:
            state["rows"] = []
    
    return state

def generate_viz_spec(state: GraphState):
    rows = state["rows"]
    query = state["query"]
    
    if not rows:
        state["viz_spec"] = {"mark": "text", "text": "No data available"}
        return state
    
    # יצירת prompt ל-Vega-Lite
    sample_data = rows[:5] if len(rows) > 5 else rows  # דוגמה של הנתונים
    
    prompt = f"""
You are a Vega-Lite stylist. Given data and user query, return a beautified Vega-Lite v5 JSON applying:

Style contract (must apply):
- width: 720, height: 420, padding: {{"left": 10, "right": 10, "top": 10, "bottom": 10}}
- background: "white"
- config:
    view: {{stroke: null}}
    font: "Inter, Arial, sans-serif"
    axis: {{
      labelFontSize: 12, titleFontSize: 13, grid: true, gridOpacity: 0.25,
      labelColor: "#334155", titleColor: "#334155", tickColor: "#CBD5E1"
    }}
    legend: {{labelFontSize: 12, titleFontSize: 13, orient: "bottom"}}
    header: {{labelFontSize: 12, titleFontSize: 13}}
- encoding defaults:
    tooltip: [{{"field":"*","type":"nominal"}}]
    x: if nominal/ordinal with long labels → labelAngle: -30, labelLimit: 140, labelOverlap: "greedy"
    y: use "quantitative" with nice: true and axis format ",.2f" or "~s" as appropriate
- color scheme: {{"scheme": "tableau10"}}
- titles: short, in Title Case
- nice margins: for bar use "cornerRadius": 4, "binSpacing": 2
- do not include data URLs; use {{"data": {{"values": ...}}}} only

User query: {query}
Sample data: {sample_data}

Return a valid Vega-Lite v5 JSON only (no prose)."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=500
        )
        
        viz_spec_text = response.choices[0].message.content.strip()
        
        # ניקוי התוצאה
        if viz_spec_text.startswith("```json"):
            viz_spec_text = viz_spec_text[7:]
        if viz_spec_text.startswith("```"):
            viz_spec_text = viz_spec_text[3:]
        if viz_spec_text.endswith("```"):
            viz_spec_text = viz_spec_text[:-3]
        
        viz_spec_text = viz_spec_text.strip()
        
        # המרה ל-JSON
        try:
            viz_spec = json.loads(viz_spec_text)
            state["viz_spec"] = viz_spec
        except json.JSONDecodeError as e:
            print(f"Error parsing Vega-Lite spec: {e}")
            # fallback ל-spec פשוט
            state["viz_spec"] = create_fallback_viz_spec(rows)
        
    except Exception as e:
        print(f"Error in generate_viz_spec: {e}")
        # fallback ל-spec פשוט
        state["viz_spec"] = create_fallback_viz_spec(rows)
    
    return state

def create_fallback_viz_spec(rows):
    """יוצר Vega-Lite spec פשוט כגיבוי"""
    if not rows:
        return {"mark": "text", "text": "No data available"}
    
    available_fields = list(rows[0].keys())
    
    # בחירת שדות מתאימים
    x_field = None
    y_field = None
    
    # חיפוש שדה טקסטואלי עבור X
    for field in ["name", "customer_name", "product_name", "category"]:
        if field in available_fields:
            x_field = field
            break
    
    # חיפוש שדה מספרי עבור Y
    for field in ["total_amount", "orders_count", "count", "amount", "price", "quantity"]:
        if field in available_fields:
            y_field = field
            break
    
    # אם לא נמצאו שדות מתאימים, נשתמש בשדות הראשונים
    if not x_field:
        x_field = available_fields[0]
    if not y_field and len(available_fields) > 1:
        y_field = available_fields[1]
    
    # יצירת spec עם styling
    if y_field:
        return {
            "width": 720,
            "height": 420,
            "background": "white",
            "padding": {"left": 10, "right": 10, "top": 10, "bottom": 10},
            "config": {
                "view": {"stroke": None},
                "font": "Inter, Arial, sans-serif",
                "axis": {
                    "labelFontSize": 12,
                    "titleFontSize": 13,
                    "grid": True,
                    "gridOpacity": 0.25,
                    "labelColor": "#334155",
                    "titleColor": "#334155",
                    "tickColor": "#CBD5E1"
                },
                "legend": {"labelFontSize": 12, "titleFontSize": 13, "orient": "bottom"}
            },
            "mark": {"type": "bar", "cornerRadius": 4, "binSpacing": 2},
            "encoding": {
                "x": {"field": x_field, "type": "ordinal", "labelAngle": -30, "labelLimit": 140},
                "y": {"field": y_field, "type": "quantitative", "nice": True, "axis": {"format": "~s"}},
                "color": {"scheme": "tableau10"}
            },
            "title": "Data Visualization"
        }
    else:
        return {
            "width": 720,
            "height": 420,
            "background": "white",
            "padding": {"left": 10, "right": 10, "top": 10, "bottom": 10},
            "mark": "text",
            "encoding": {
                "text": {"field": x_field, "type": "nominal"}
            },
            "title": "Data List"
        }

def decide_action(state: GraphState):
    q = state["query"].lower()
    if "download" in q or "export" in q or "save" in q:
        state["action"] = "download"
    else:
        state["action"] = "display"
    return state

def download_node(state: GraphState):
    path = save_to_csv(state["rows"])
    state["file_path"] = path
    return state

def display_node(state: GraphState):
    return state

# ---------- Build Graph ----------

def build_graph():
    graph = StateGraph(GraphState)

    graph.add_node("detect_intent", detect_intent)
    graph.add_node("generate_sql", generate_sql)
    graph.add_node("apply_sql_guardrails", apply_sql_guardrails)
    graph.add_node("execute_sql", execute_sql)
    graph.add_node("generate_viz_spec", generate_viz_spec)
    graph.add_node("decide_action", decide_action)
    graph.add_node("download_node", download_node)
    graph.add_node("display_node", display_node)

    graph.set_entry_point("detect_intent")

    graph.add_conditional_edges(
        "detect_intent",
        lambda s: s["intent"],
        {
            "sql": "generate_sql",
            "viz": "generate_sql",
        }
    )

    graph.add_edge("generate_sql", "apply_sql_guardrails")
    
    # בדיקת guardrails - אם נחסם, עובר ישר ל-display
    graph.add_conditional_edges(
        "apply_sql_guardrails",
        lambda s: s.get("guardrail_ok", True),
        {
            True: "execute_sql",
            False: "display_node"
        }
    )

    # branch viz
    graph.add_conditional_edges(
        "execute_sql",
        lambda s: s["intent"],
        {
            "sql": "decide_action",
            "viz": "generate_viz_spec",
        }
    )

    graph.add_edge("generate_viz_spec", "decide_action")

    graph.add_conditional_edges(
        "decide_action",
        lambda s: s["action"],
        {
            "download": "download_node",
            "display": "display_node"
        }
    )

    graph.add_edge("download_node", END)
    graph.add_edge("display_node", END)

    return graph.compile()
