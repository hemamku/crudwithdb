from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os

app = FastAPI(
    title="PostgreSQL CRUD API",
    version="1.0.0"
)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("DATABASE_URL not set")

# -----------------------------
# DB CONNECTION
# -----------------------------
def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )

# -----------------------------
# MODEL
# -----------------------------
class User(BaseModel):
    name: str

# -----------------------------
# INIT DB
# -----------------------------
@app.on_event("startup")
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

# -----------------------------
# CREATE USER
# -----------------------------
@app.post("/users", description="Create a new user in PostgreSQL database")
def create_user(user: User):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO users (name) VALUES (%s) RETURNING id;",
        (user.name,)
    )

    user_id = cur.fetchone()["id"]

    conn.commit()
    cur.close()
    conn.close()

    return {"id": user_id, "name": user.name}

# -----------------------------
# GET USERS
# -----------------------------
@app.get("/users", description="Fetch all users from PostgreSQL database")
def get_users():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM users ORDER BY id;")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows

# -----------------------------
# GET USER
# -----------------------------
@app.get("/users/{user_id}", description="Fetch a single user by ID")
def get_user(user_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM users WHERE id=%s;", (user_id,))
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return row

# -----------------------------
# UPDATE USER
# -----------------------------
@app.put("/users/{user_id}", description="Update an existing user by ID")
def update_user(user_id: int, user: User):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "UPDATE users SET name=%s WHERE id=%s RETURNING id;",
        (user.name, user_id)
    )

    updated = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    if not updated:
        raise HTTPException(status_code=404, detail="User not found")

    return {"id": user_id, "name": user.name}

# -----------------------------
# DELETE USER
# -----------------------------
@app.delete("/users/{user_id}", description="Delete a user by ID")
def delete_user(user_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM users WHERE id=%s RETURNING id;", (user_id,))
    deleted = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    if not deleted:
        raise HTTPException(status_code=404, detail="User not found")

    return {"message": f"User {user_id} deleted"}