import psycopg2
import json
import re

THRESHOLDS = {
    "require_limit": True,
    "max_root_rows": 10_000,
    "max_node_bytes": 50_000_000,  # 50MB אומדן
    "max_seqscan_rows": 10_000,
    "max_sort_rows_no_limit": 10_000,
    "max_nested_loop_sides": 5_000,
    "default_limit": 10,
}

def explain_json(conn, sql):
    """מריץ EXPLAIN (FORMAT JSON) על השאילתה"""
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (FORMAT JSON) " + sql)
        # psycopg2 מחזיר מערך עם JSON יחיד
        return cur.fetchone()[0][0]  # dict: {"Plan": {...}, ...}

def has_limit_node(plan):
    """בודק אם יש Limit node בעץ התוכנית"""
    if plan.get("Node Type") == "Limit":
        return True
    for child in plan.get("Plans", []) or []:
        if has_limit_node(child):
            return True
    return False

def walk_plan(plan, findings):
    """עובר על כל ה-nodes בעץ התוכנית ומאסוף מידע"""
    rows = int(plan.get("Plan Rows", 0) or 0)
    width = int(plan.get("Plan Width", 0) or 0)
    node_type = plan.get("Node Type", "")
    
    findings["max_node_bytes"] = max(findings["max_node_bytes"], rows * width)
    findings["max_node_rows"] = max(findings["max_node_rows"], rows)

    # Seq Scan כבד
    if node_type == "Seq Scan" and rows > THRESHOLDS["max_seqscan_rows"]:
        findings["seq_scans_heavy"].append({
            "relation": plan.get("Relation Name"),
            "rows": rows,
            "filter": plan.get("Filter")
        })

    # Sort ללא Limit מעל
    if node_type == "Sort" and rows > THRESHOLDS["max_sort_rows_no_limit"]:
        findings["sort_nodes"].append({"rows": rows})

    # Nested Loop כבד
    if node_type == "Nested Loop":
        left_rows = right_rows = 0
        ch = plan.get("Plans", []) or []
        if len(ch) >= 1: 
            left_rows = int(ch[0].get("Plan Rows", 0) or 0)
        if len(ch) >= 2: 
            right_rows = int(ch[1].get("Plan Rows", 0) or 0)
        
        if (left_rows > THRESHOLDS["max_nested_loop_sides"] and 
            right_rows > THRESHOLDS["max_nested_loop_sides"]):
            findings["nested_loop_heavy"].append({"left": left_rows, "right": right_rows})
        
        # Cross join חשוד: אין תנאי Hash/Merge ולא Join Filter
        has_cond = any(k in plan for k in ("Hash Cond", "Merge Cond"))
        if not has_cond and not plan.get("Join Filter"):
            findings["possible_cross_join"] = True

    # HashAggregate/GroupAggregate גדול
    if node_type in ["HashAggregate", "GroupAggregate"] and rows > 50_000:
        findings["large_aggregates"].append({"type": node_type, "rows": rows})

    # עבור על ילדים
    for child in plan.get("Plans", []) or []:
        walk_plan(child, findings)

def add_limit_to_sql(sql, limit_value=None):
    """מוסיף LIMIT לשאילתה אם אין"""
    if limit_value is None:
        limit_value = THRESHOLDS["default_limit"]
    
    # בדיקה אם יש כבר LIMIT
    sql_upper = sql.upper()
    if "LIMIT" in sql_upper:
        return sql, False  # כבר יש LIMIT
    
    # הוספת LIMIT בסוף
    sql = sql.rstrip().rstrip(';')
    sql_with_limit = f"{sql} LIMIT {limit_value};"
    
    return sql_with_limit, True

def explain_guardrail(conn, sql, thresholds=THRESHOLDS):
    """מריץ guardrail על השאילתה"""
    findings = {
        "limit_present": False,
        "root_rows": 0,
        "max_node_bytes": 0,
        "max_node_rows": 0,
        "seq_scans_heavy": [],
        "sort_nodes": [],
        "nested_loop_heavy": [],
        "large_aggregates": [],
        "possible_cross_join": False,
        "reasons": [],
        "notices": []
    }
    
    try:
        ok = True
        plan_root = explain_json(conn, sql)["Plan"]
        print(plan_root)
        print(sql)
        findings["limit_present"] = has_limit_node(plan_root)
        findings["root_rows"] = int(plan_root.get("Plan Rows", 0) or 0)
        walk_plan(plan_root, findings)
        # כללים
        if thresholds["require_limit"] and not findings["limit_present"]:
            findings["notices"].append("Query has no LIMIT, adding LIMIT.")

        if findings["root_rows"] > thresholds["max_root_rows"]:
            findings["reasons"].append(f"Estimated result too large ({findings['root_rows']} rows).")
            ok = False

        if findings["max_node_bytes"] > thresholds["max_node_bytes"]:
            findings["reasons"].append("Operators process too many bytes (estimated).")
            ok = False
            
        if findings["seq_scans_heavy"]:
            findings["reasons"].append("Heavy sequential scans on large tables.")
            ok = False
            
        if findings["sort_nodes"] and not findings["limit_present"]:
            findings["reasons"].append("Large SORT without LIMIT upstream.")
            ok = False
            
        if findings["nested_loop_heavy"]:
            findings["reasons"].append("Heavy nested loop join (both sides large).")
            ok = False
            
        if findings["large_aggregates"]:
            ok = False
            findings["reasons"].append("Large aggregation operations.")
            
        if findings["possible_cross_join"]:
            findings["reasons"].append("Possible cross join (no join condition found).")
            ok = False

        return ok, findings
        
    except Exception as e:
        findings["reasons"].append(f"EXPLAIN failed: {str(e)}")
        return False, findings

def apply_guardrails(conn, sql, thresholds=THRESHOLDS):
    """מחיל guardrails על השאילתה ומחזיר שאילתה מתוקנת"""
    notices = []
    reasons = []
    
    # בדיקת guardrails ישירות על השאילתה (LIMIT כבר נוסף ב-generate_sql)
    ok, findings = explain_guardrail(conn, sql, thresholds)
    
    # הוספת notices מה-guardrails
    notices.extend(findings.get("notices", []))
    reasons.extend(findings.get("reasons", []))
    
    print(5,reasons)
    print(6,findings)
    result = {
        "sql": sql,
        "ok": ok,
        "findings": findings,
        "limit_added": False,  # LIMIT נוסף ב-generate_sql
        "notices": notices,
        "reasons": reasons,
    }

    print(result)
    
    return result
