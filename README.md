# Filer

Tracks files on your filesystem, by content hashes.

This maintains a mapping from file path to content hashes (eg, SHA-512), and
back again.  It also maintains a historical mapping, so it can tell you what
mappings existed at some point in the past.

## Features

### Initial set

 - Configure set of top level paths to trawl
 - Configure set of directory or path patterns to ignore
 - Uses inotify to avoid excessive re-crawling
 - Waits for files to "settle" before recording them - avoiding computing hashes of short lived temporary files.
 - Batch queries - look up a list of paths or hashes in a single call
 - Support for symlinks - looking up a path that includes symlinks works (as long as the list of paths being monitored includes the destinations of all intermediate symlinks.
 - Persistent data (using SQLite)
 - Updating process independent from API process, so calls to one don't block the other.

## Limitations

 - OS support: only tested on Linux so far
 - No binary API - only JSON
 - No support for case folding - assumes path with different cases are always identical
