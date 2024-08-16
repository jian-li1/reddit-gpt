# Inherited from 
# https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/to_csv.py
import zstandard
import os
import json
import sys
from datetime import datetime
import logging.handlers
import sqlite3
from collections import OrderedDict

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)
	
def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]
		reader.close()
		
if __name__ == "__main__":
	if len(sys.argv) != 3:
		log.error(f"Usage: {sys.argv[0]} <zst file> <db file>")
		sys.exit(1)
	
	input_file_path = sys.argv[1]
	output_file_path = sys.argv[2]

	is_submission = "submission" in input_file_path
	if is_submission:
		# fields = ["author","title","score","created","text","id","link_flair_text","distinguished","subreddit","link"]
		fields = OrderedDict([
			("id", "id TEXT UNIQUE PRIMARY KEY"),
			("author", "author TEXT"),
			("title", "title TEXT"),
			("score", "score INTEGER"),
			("created", "created TEXT"),
			("text", "body TEXT"),
			("link_flair_text", "flair TEXT"),
			("distinguished", "distinguished TEXT"),
			("subreddit", "subreddit TEXT"),
			("link", "link TEXT")
		])
	else:
		# fields = ["author","score","created","body","name","parent_id","distinguished","subreddit","link"]
		fields = OrderedDict([
			("name", "id TEXT UNIQUE PRIMARY KEY"),
			("parent_id", "parent_id TEXT"),
			("link_id", "post_id TEXT"),
			("author", "author TEXT"),
			("score", "score INTEGER"),
			("created", "created TEXT"),
			("body", "body TEXT"),
			("distinguished", "distinguished TEXT"),
			("subreddit", "subreddit TEXT"),
			("link", "link TEXT")
		])

	file_size = os.stat(input_file_path).st_size
	file_lines, bad_lines = 0, 0
	line, created = None, None

	conn = sqlite3.connect(output_file_path)
	curr = conn.cursor()
	columns = ", ".join(fields.values())
	curr.execute(f"CREATE TABLE IF NOT EXISTS {'submissions' if is_submission else 'comments'} ({columns})")
	conn.commit()

	try:
		for line, file_bytes_processed in read_lines_zst(input_file_path):
			try:
				obj = json.loads(line)
				# output_obj = []
				output_dict = OrderedDict()
				for field in fields.keys():
					if field == "created":
						value = datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d %H:%M")
					elif field == "link":
						if 'permalink' in obj:
							value = f"https://www.reddit.com{obj['permalink']}"
						else:
							value = f"https://www.reddit.com/r/{obj['subreddit']}/comments/{obj['link_id'][3:]}/_/{obj['id']}/"
					elif field == "author":
						value = f"u/{obj['author']}"
					elif field == "text":
						if 'selftext' in obj:
							value = obj['selftext']
						else:
							value = ""
					elif field == "name" and not "name" in obj:
						value = f"t1_{obj['id']}"
					elif field == "id" and is_submission:
						value = f"t3_{obj[field]}"
					elif field == "distinguished" and not "distinguished" in obj:
						value = None
					else:
						value = obj[field]

					# output_obj.append(str(value).encode("utf-8", errors='replace').decode())
					col_name = fields[field].split()[0]
					output_dict[col_name] = str(value).encode("utf-8", errors='replace').decode()
				
				columns = ", ".join([col.split()[0] for col in fields.values()])
				placeholders = ", ".join("?" * len(fields))
				curr.execute(f"INSERT INTO {'submissions' if is_submission else 'comments'} ({columns}) VALUES ({placeholders})", tuple(output_dict.values()))

				created = datetime.utcfromtimestamp(int(obj['created_utc']))
			except (json.JSONDecodeError, sqlite3.Error) as err:
				bad_lines += 1
				
			file_lines += 1
			if file_lines % 10000 == 0:
				conn.commit()
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")
	except KeyError as err:
		log.info(f"Object has no key: {err}")
		log.info(line)
	except Exception as err:
		log.info(err)
		log.info(line)

	conn.commit()
	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
	curr.close()
	conn.close()