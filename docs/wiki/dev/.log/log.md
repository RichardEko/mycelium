# dev/.log — ingest history

One dated file per ingest: `YYYY-MM-DD-<slug>.md`, containing
`## [YYYY-MM-DD] ingest | <topic>` plus a few lines on what changed and which pages were
touched. Never append to an existing file. Lint passes are logged the same way
(`YYYY-MM-DD-lint.md`).
