import sqlite3

con = sqlite3.connect("data/library/db.sqlite3")
for name, sql in con.execute("SELECT name, sql FROM sqlite_master WHERE type='table'"):
    print(f"TABLE {name}\n{sql}\n")
