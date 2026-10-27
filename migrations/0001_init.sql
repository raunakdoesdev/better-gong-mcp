CREATE TABLE calls (
  id TEXT PRIMARY KEY,
  title TEXT,
  url TEXT,
  started TEXT,
  started_ms INTEGER,
  duration_s INTEGER,
  scope TEXT,
  direction TEXT,
  system TEXT,
  media TEXT,
  language TEXT,
  account_id TEXT,
  account_name TEXT,
  brief TEXT,
  key_points TEXT,
  highlights TEXT,
  outline TEXT,
  topics TEXT,
  trackers TEXT,
  call_outcome TEXT,
  synced_at INTEGER
);
CREATE INDEX calls_started ON calls(started_ms);
CREATE INDEX calls_account ON calls(account_id);

CREATE TABLE parties (
  call_id TEXT NOT NULL,
  speaker_id TEXT,
  name TEXT,
  email TEXT,
  title TEXT,
  affiliation TEXT,
  user_id TEXT
);
CREATE INDEX parties_call ON parties(call_id);
CREATE INDEX parties_email ON parties(email);

CREATE TABLE accounts (
  id TEXT PRIMARY KEY,
  name TEXT,
  website TEXT,
  industry TEXT,
  segment TEXT,
  employees INTEGER,
  description TEXT,
  fields TEXT
);
CREATE INDEX accounts_name ON accounts(name);

CREATE TABLE chunks (
  call_id TEXT NOT NULL,
  idx INTEGER NOT NULL,
  start_s REAL,
  end_s REAL,
  text TEXT NOT NULL,
  UNIQUE (call_id, idx)
);

CREATE VIRTUAL TABLE chunks_fts USING fts5(text, content='chunks', content_rowid='rowid', tokenize='porter unicode61');
CREATE TRIGGER chunks_ai AFTER INSERT ON chunks BEGIN
  INSERT INTO chunks_fts(rowid, text) VALUES (new.rowid, new.text);
END;
CREATE TRIGGER chunks_ad AFTER DELETE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
END;
