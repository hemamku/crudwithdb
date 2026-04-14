from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(
    title="PostgreSQL CRUD API",
    version="1.0.0"
)

# -----------------------------
# DB CONNECTION (SAFE)
# -----------------------------
def get_conn():
    try:
        return psycopg2.connect(
            dbname="orchestrate_db",
            user="hemdurai",   # keep your working DB user
            password="",
            host=os.getenv("DB_HOST", "localhost"),
            port="5432",
            cursor_factory=RealDictCursor
        )
    except Exception as e:
        print("❌ DB CONNECTION ERROR:", e)
        raise HTTPException(status_code=500, detail="Database connection failed")

# -----------------------------
# REQUEST MODEL
# -----------------------------
class User(BaseModel):
    name: str

# -----------------------------
# INIT DB
# -----------------------------
@app.on_event("startup")
def init_db():
    try:
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

    except Exception as e:
        print("❌ INIT DB ERROR:", e)

# -----------------------------
# CREATE USER
# -----------------------------
@app.post("/users", description="Create a new user in PostgreSQL database")
def create_user(user: User):
    try:
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

    except Exception as e:
        print("❌ CREATE USER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# GET ALL USERS
# -----------------------------
@app.get("/users", description="Fetch all users from PostgreSQL database")
def get_users():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id, name FROM users ORDER BY id;")
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows

    except Exception as e:
        print("❌ GET USERS ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# GET USER BY ID
# -----------------------------
@app.get("/users/{user_id}", description="Fetch a single user by ID")
def get_user(user_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "SELECT id, name FROM users WHERE id=%s;",
            (user_id,)
        )
        row = cur.fetchone()

        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return row

    except HTTPException:
        raise
    except Exception as e:
        print("❌ GET USER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# UPDATE USER
# -----------------------------
@app.put("/users/{user_id}", description="Update an existing user by ID")
def update_user(user_id: int, user: User):
    try:
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

    except HTTPException:
        raise
    except Exception as e:
        print("❌ UPDATE USER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# DELETE USER
# -----------------------------
@app.delete("/users/{user_id}", description="Delete a user by ID")
def delete_user(user_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "DELETE FROM users WHERE id=%s RETURNING id;",
            (user_id,)
        )

        deleted = cur.fetchone()

        conn.commit()
        cur.close()
        conn.close()

        if not deleted:
            raise HTTPException(status_code=404, detail="User not found")

        return {"message": f"User {user_id} deleted"}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ DELETE USER ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))
