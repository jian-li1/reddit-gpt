import sqlite3
from collections import OrderedDict
import logging.handlers
from datetime import datetime
import sys

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

# LRU cache to store recently accessed entries so a SQL query is not needed
class Cache:
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = max(1, capacity)
    
    def get(self, key):
        val = self.cache.get(key)
        if val:
            self.cache.move_to_end(key)
        return val
    
    def __getitem__(self, key):
        val = self.get(key)
        if not val:
            raise KeyError(key)
        return val
    
    def __setitem__(self, key, val):
        self.cache[key] = val
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

def valid(data):
    if data["score"] < 5:
        return False
    elif data["body"].startswith("[removed]") or data["body"].startswith("[deleted]"):
        return False
    elif data["distinguished"] == "moderator":
        return False
    return True

def get_submission(id):
    post = cached_submissions.get(id)
    if not post:
        curr.execute(f"SELECT * FROM submissions WHERE id = ? LIMIT 1", (id,))
        if not (post := curr.fetchone()):
            return None
        cached_submissions[id] = post = dict(post)
    return post

def get_comment(id):
    comment = cached_comments.get(id)
    if not comment:
        curr.execute(f"SELECT * FROM comments WHERE id = ? LIMIT 1", (id,))
        if not (comment := curr.fetchone()):
            return None
        cached_submissions[id] = comment = dict(comment)
    return comment

cached_submissions = Cache(10000)
cached_comments = Cache(10000)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        log.error(f"Usage: {sys.argv[0]} <db file>")
        sys.exit(1)
    
    db_file = sys.argv[1]
    # flairs = ["Question", "Need Advice"]

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    curr = conn.cursor()
    curr.execute("CREATE TABLE IF NOT EXISTS train (id TEXT, output_id TEXT, input TEXT, output TEXT)")

    batch_size = 10000
    offset = 0

    curr.execute("SELECT COUNT(*) FROM comments")
    total_lines = curr.fetchone()[0]

    curr.execute("SELECT * FROM comments LIMIT ? OFFSET ?", (batch_size, offset))
    pairs = 0
    lines = 0
    
    while rows := curr.fetchall():
        for row in rows:
            row = dict(row)

            if row["parent_id"].startswith("t1"):
                parent = get_comment(row["parent_id"])
                input_type = "comment"
            elif row["parent_id"].startswith("t3"):
                parent = get_submission(row["parent_id"])
                input_type = "post"
            
            if parent and valid(row) and valid(parent):
                input_text = f"{parent['title']}\n\n{parent['body']}".rstrip(None).rstrip() if row["parent_id"].startswith("t3") else parent["body"]
                curr.execute("INSERT INTO train (id, output_id, input, output) VALUES (?, ?, ?, ?)",\
                            (row["parent_id"], row["id"], input_text, row["body"]))
                pairs += 1
            
            lines += 1
            if pairs % 10000 == 0:
                conn.commit()
            if lines % 10000 == 0:
                log.info(f"{row['created']} : {pairs:,} : {lines:,} : {(lines / total_lines) * 100:.0f}%")
            
        
        offset += batch_size
        curr.execute("SELECT * FROM comments LIMIT ? OFFSET ?", (batch_size, offset))

    conn.commit()
    log.info(f"Complete : {pairs:,}")
    curr.close()
    conn.close()
