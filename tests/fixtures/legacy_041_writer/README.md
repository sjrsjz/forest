# Actual 041 BotStore writer snapshot

`bot.py.gz` contains the actual pre-042 `src/forest/storage/bot.py` source,
captured as UTF-8/LF from the frozen 041 control integration tree before the
writer-v2 implementation. It is not generated from the new BotStore or from
52207c0's older 038 writer. Gzip mtime is zero.

Verified provenance: commit `f86ba34`, path `src/forest/storage/bot.py`.
The decompressed bytes match that actual Git blob exactly. The test needs no
Git command at runtime.

- Decompressed bytes: 123694
- SHA256: `8960f47437997394d2b07aa9f9bdbc2616da4d6a34b741f029ff170d072934dc`
- Actual connection startup option: `forest.writer_generation=1`, imported from
  the unchanged `migration_v1.py`.

Tests execute this source unchanged in its own module namespace. Only its
resource lookup is bound to the checksum-verified 001–041 resource snapshot;
the source's own migration, pool, acquire and renew implementations execute.
Old SQL/handler/helper resources are checked against the frozen full 041 chain
digest before being used. There is no test-time Git or adjacent-tree dependency.
