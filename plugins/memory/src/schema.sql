CREATE TABLE IF NOT EXISTS trunks (group_id INTEGER PRIMARY KEY, next_sequence INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS units (
    group_id INTEGER NOT NULL, sequence INTEGER NOT NULL, source_key TEXT NOT NULL,
    content TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(group_id, sequence), UNIQUE(group_id, source_key)
);
CREATE INDEX IF NOT EXISTS archived_units ON units(group_id, archived, sequence);
CREATE TABLE IF NOT EXISTS links (
    trigger_group INTEGER NOT NULL, trigger_seq INTEGER NOT NULL,
    recalled_group INTEGER NOT NULL, recalled_seq INTEGER NOT NULL,
    PRIMARY KEY(trigger_group, trigger_seq, recalled_group, recalled_seq),
    FOREIGN KEY(trigger_group, trigger_seq) REFERENCES units(group_id,sequence) ON DELETE CASCADE,
    FOREIGN KEY(recalled_group, recalled_seq) REFERENCES units(group_id,sequence) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS links_reverse ON links(recalled_group,recalled_seq);
CREATE TABLE IF NOT EXISTS media (
    reference TEXT PRIMARY KEY, group_id INTEGER NOT NULL, sequence INTEGER NOT NULL,
    path TEXT NOT NULL, source TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', error TEXT, mime TEXT,
    FOREIGN KEY(group_id,sequence) REFERENCES units(group_id,sequence) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS media_pending ON media(state);
CREATE TABLE IF NOT EXISTS vectors (
    space TEXT NOT NULL, point_id TEXT NOT NULL, group_id INTEGER NOT NULL, sequence INTEGER NOT NULL,
    representation TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', error TEXT,
    PRIMARY KEY(space,point_id),
    FOREIGN KEY(group_id,sequence) REFERENCES units(group_id,sequence) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS vectors_pending ON vectors(space,state);
CREATE TABLE IF NOT EXISTS deliveries (
    self_id INTEGER NOT NULL, group_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
    turn_sequence INTEGER NOT NULL, PRIMARY KEY(self_id,group_id,message_id)
);
