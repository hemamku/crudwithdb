from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title="PostgreSQL MCP CRUD API",
    version="1.0.0",
    description="MCP-compatible FastAPI service for performing CRUD operations on a PostgreSQL database used by IBM Orchestrate agents."
)

# -----------------------------
# DATABASE CONFIG
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("DATABASE_URL not set")

print("DB URL:", DATABASE_URL)
def get_conn():
    """
    Creates a new PostgreSQL connection.
    Used by MCP tools to execute CRUD operations.
    """
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require"
    )


# -----------------------------
# DATA MODEL
# -----------------------------
class User(BaseModel):
    name: str


# -----------------------------
# INIT DB (startup)
# -----------------------------
@app.on_event("startup")
def init_db():
    """
    Initializes users table if it does not exist.
    Runs once when FastAPI server starts.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()


# -----------------------------
# CREATE USER
# -----------------------------
@app.post(
    "/users",
    description="Create a new user in PostgreSQL database via MCP FastAPI service."
)
def create_user(user: User):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (name) VALUES (%s) RETURNING id;",
            (user.name,)
        )
        user_id = cur.fetchone()["id"]
        conn.commit()
        return {"id": user_id, "name": user.name}
    finally:
        cur.close()
        conn.close()


# -----------------------------
# GET ALL USERS
# -----------------------------
@app.get(
    "/users",
    description="Fetch all users from PostgreSQL database via MCP FastAPI service."
)
def get_users():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users ORDER BY id;")
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()


# -----------------------------
# GET USER BY ID
# -----------------------------
@app.get(
    "/users/{user_id}",
    description="Fetch a single user by ID from PostgreSQL database."
)
def get_user(user_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users WHERE id=%s;", (user_id,))
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return row
    finally:
        cur.close()
        conn.close()


# -----------------------------
# UPDATE USER
# -----------------------------
@app.put(
    "/users/{user_id}",
    description="Update an existing user in PostgreSQL database via MCP FastAPI service."
)
def update_user(user_id: int, user: User):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET name=%s WHERE id=%s RETURNING id;",
            (user.name, user_id)
        )
        updated = cur.fetchone()
        conn.commit()

        if not updated:
            raise HTTPException(status_code=404, detail="User not found")

        return {"id": user_id, "name": user.name}
    finally:
        cur.close()
        conn.close()


# -----------------------------
# DELETE USER
# -----------------------------
@app.delete(
    "/users/{user_id}",
    description="Delete a user from PostgreSQL database via MCP FastAPI service."
)
def delete_user(user_id: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM users WHERE id=%s RETURNING id;",
            (user_id,)
        )
        deleted = cur.fetchone()
        conn.commit()

        if not deleted:
            raise HTTPException(status_code=404, detail="User not found")

        return {"message": f"User {user_id} deleted"}
    finally:
        cur.close()
        conn.close()