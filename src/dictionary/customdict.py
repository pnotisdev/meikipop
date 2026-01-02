# customdict.py
import json
import logging
import pickle
import time
import os
import sqlite3
import concurrent.futures
from collections import defaultdict
from typing import List, Any

from src.config.config import IS_WINDOWS, config
from src.dictionary.yomichan import parse_yomichan_zip, parse_yomichan_dir

logger = logging.getLogger(__name__) # Get the logger

class CompactEntry:
    __slots__ = ('id', 'kebs', 'rebs', 'senses', 'raw_k_ele', 'raw_r_ele', 'raw_sense')
    def __init__(self, id, kebs, rebs, senses, raw_k_ele, raw_r_ele, raw_sense):
        self.id = id
        self.kebs = kebs
        self.rebs = rebs
        self.senses = senses
        self.raw_k_ele = raw_k_ele
        self.raw_r_ele = raw_r_ele
        self.raw_sense = raw_sense
    
    def __getitem__(self, key):
        return getattr(self, key)
        
    def get(self, key, default=None):
        return getattr(self, key, default)

    def __setitem__(self, key, value):
        setattr(self, key, value)

class SqliteEntryList:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        # Cache the length
        self.cursor.execute("SELECT COUNT(*) FROM entries")
        self._len = self.cursor.fetchone()[0]

    def __len__(self):
        return self._len

    def __getitem__(self, index):
        if index < 0 or index >= self._len:
            raise IndexError("list index out of range")
        
        # Fetch blob
        cursor = self.conn.cursor()
        cursor.execute("SELECT data FROM entries WHERE id = ?", (index,))
        row = cursor.fetchone()
        if row:
            return pickle.loads(row[0])
        raise IndexError(f"Entry {index} not found in DB")

    def __iter__(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT data FROM entries ORDER BY id")
        while True:
            rows = cursor.fetchmany(1000)
            if not rows: break
            for row in rows:
                yield pickle.loads(row[0])

class SqliteLookupMap:
    def __init__(self, conn: sqlite3.Connection, table_name: str):
        self.conn = conn
        self.table_name = table_name

    def get(self, key: str, default=None):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT entry_ids FROM {self.table_name} WHERE text = ?", (key,))
        row = cursor.fetchone()
        if row:
            return pickle.loads(row[0])
        return default

class Dictionary:
    def __init__(self):
        self.entries = []
        self.lookup_kan = defaultdict(list)
        self.lookup_kana = defaultdict(list)
        self.deconjugator_rules = []
        self.priority_map = {}
        self.frequency_map = {}
        self._is_loaded = False

    def import_jmdict_json(self, json_paths: list[str]):
        all_jmdict_entries = []
        for path in sorted(json_paths):
            with open(path, 'r', encoding='utf-8') as f:
                all_jmdict_entries.extend(json.load(f))
        for entry_data in all_jmdict_entries:
            kebs = [k['keb'] for k in entry_data.get('k_ele', [])]
            rebs = [r['reb'] for r in entry_data.get('r_ele', [])]
            senses_processed = []
            last_pos = []
            for sense in entry_data.get('sense', []):
                glosses = [g for g in sense.get('gloss', [])]
                pos = sense.get('pos', last_pos)
                last_pos = pos
                if glosses:
                    senses_processed.append({'glosses': glosses, 'pos': [p.strip('&;') for p in pos]})
            if not (kebs or rebs) or not senses_processed:
                continue
            entry = CompactEntry(entry_data['seq'], kebs, rebs, senses_processed, entry_data.get('k_ele', []), entry_data.get('r_ele', []), entry_data.get('sense', []))
            self.entries.append(entry)
            entry_index = len(self.entries) - 1
            for keb in kebs:
                self.lookup_kan[keb].append(entry_index)
            for reb in rebs:
                self.lookup_kana[reb].append(entry_index)

    def import_deconjugator(self, json_path: str):
        with open(json_path, 'r', encoding='utf-8') as f:
            rules = json.load(f)
            self.deconjugator_rules = [r for r in rules if isinstance(r, dict)]

    def import_priority(self, json_path: str):
        with open(json_path, 'r', encoding='utf-8') as f:
            priority_data = json.load(f)
            for item in priority_data:
                key = (item[0], item[1])
                self.priority_map[key] = item[2]

    def import_yomichan_directory(self, directory_path: str):
        """Imports all Yomichan/Yomitan dictionaries (.zip or folder) from a directory."""
        if not os.path.exists(directory_path):
            logger.warning(f"Yomichan dictionary directory not found: {directory_path}")
            return

        logger.info(f"Scanning for Yomichan dictionaries in: {directory_path}")
        
        # Find zip files
        available_zips = [f for f in os.listdir(directory_path) if f.lower().endswith('.zip')]
        
        # Find directories that look like dictionaries (have index.json)
        available_dirs = []
        for f in os.listdir(directory_path):
            full_path = os.path.join(directory_path, f)
            if os.path.isdir(full_path) and os.path.exists(os.path.join(full_path, 'index.json')):
                available_dirs.append(f)

        all_available = available_zips + available_dirs
        
        if config.enabled_dictionaries is None:
            # Default: load all
            files_to_load = all_available
        else:
            # Load only enabled ones
            files_to_load = [f for f in all_available if f in config.enabled_dictionaries]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for filename in files_to_load:
                full_path = os.path.join(directory_path, filename)
                if os.path.isdir(full_path):
                    futures.append(executor.submit(self._load_yomichan_folder_entries, full_path))
                else:
                    futures.append(executor.submit(self._load_yomichan_zip_entries, full_path))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        entries, source_name = result
                        self._add_entries(entries, source_name)
                except Exception as e:
                    logger.error(f"Error loading dictionary: {e}")

    def import_yomichan_zip(self, zip_path: str):
        """Imports a single Yomichan/Yomitan dictionary ZIP."""
        result = self._load_yomichan_zip_entries(zip_path)
        if result:
            self._add_entries(*result)

    def import_yomichan_folder(self, dir_path: str):
        """Imports a single Yomichan/Yomitan dictionary directory."""
        result = self._load_yomichan_folder_entries(dir_path)
        if result:
            self._add_entries(*result)

    def _load_yomichan_zip_entries(self, zip_path: str):
        cache_path = zip_path + ".cache.pkl"
        cached = self._load_entries_from_cache(zip_path, cache_path)
        if cached:
            return cached, os.path.basename(zip_path) + " (Cached)"
        
        result = parse_yomichan_zip(zip_path)
        if result and (result[0] or result[1]):
            self._save_to_cache(result, cache_path)
            return result, os.path.basename(zip_path)
        return None

    def _load_yomichan_folder_entries(self, dir_path: str):
        cache_path = os.path.join(dir_path, "dictionary.cache.pkl")
        cached = self._load_entries_from_cache(dir_path, cache_path)
        if cached:
            return cached, os.path.basename(dir_path) + " (Cached)"
            
        result = parse_yomichan_dir(dir_path)
        if result and (result[0] or result[1]):
            self._save_to_cache(result, cache_path)
            return result, os.path.basename(dir_path)
        return None

    def _load_entries_from_cache(self, source_path: str, cache_path: str) -> tuple | None:
        """Attempts to load entries from cache. Returns entries if successful, else None."""
        if not os.path.exists(cache_path):
            return None
        
        try:
            # Check modification times
            source_mtime = os.path.getmtime(source_path)
            cache_mtime = os.path.getmtime(cache_path)
            
            if source_mtime > cache_mtime:
                logger.info(f"Cache outdated for {source_path}")
                return None

            logger.info(f"Loading cache from {cache_path}")
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            
            if isinstance(data, list):
                return data, {}
            
            return data
        except Exception as e:
            logger.warning(f"Failed to load cache {cache_path}: {e}")
            return None

    def _load_from_cache(self, source_path: str, cache_path: str) -> bool:
        # Deprecated, kept for compatibility if needed, but internal usage replaced
        entries = self._load_entries_from_cache(source_path, cache_path)
        if entries:
            self._add_entries(entries, os.path.basename(source_path) + " (Cached)")
            return True
        return False

    def _save_to_cache(self, data: tuple, cache_path: str):
        """Saves entries to cache."""
        try:
            logger.info(f"Saving cache to {cache_path}")
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to save cache {cache_path}: {e}")

    def _add_entries(self, data: tuple, source_name: str):
        """Helper to add entries to the dictionary and update lookups."""
        new_entries, frequency_map = data
        
        start_index = len(self.entries)
        
        # Convert dict entries to CompactEntry if needed
        compact_entries = []
        for entry in new_entries:
            if isinstance(entry, dict):
                compact_entries.append(CompactEntry(
                    entry['id'], entry['kebs'], entry['rebs'], entry['senses'],
                    entry.get('raw_k_ele', []), entry.get('raw_r_ele', []), entry.get('raw_sense', [])
                ))
            else:
                compact_entries.append(entry)
                
        self.entries.extend(compact_entries)
        
        # Update lookups
        for i, entry in enumerate(compact_entries):
            real_index = start_index + i
            for keb in entry.kebs:
                self.lookup_kan[keb].append(real_index)
            for reb in entry.rebs:
                self.lookup_kana[reb].append(real_index)
        
        if frequency_map:
            self.frequency_map.update(frequency_map)
        
        logger.info(f"Imported {len(new_entries)} entries and {len(frequency_map)} frequency items from {source_name}")

    def save_dictionary(self, file_path: str):
        data_to_save = {'entries': self.entries, 'lookup_kan': self.lookup_kan, 'lookup_kana': self.lookup_kana, 'deconjugator_rules': self.deconjugator_rules, 'priority_map': self.priority_map, 'frequency_map': self.frequency_map}
        with open(file_path, 'wb') as f:
            pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

    def convert_to_sqlite(self, db_path: str):
        """Converts the currently loaded in-memory dictionary to an SQLite database."""
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except OSError:
                logger.warning(f"Could not remove existing DB {db_path}, trying to overwrite.")
            
        logger.info(f"Converting dictionary to SQLite: {db_path}...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY, data BLOB)")
        cursor.execute("CREATE TABLE lookup_kan (text TEXT PRIMARY KEY, entry_ids BLOB)")
        cursor.execute("CREATE TABLE lookup_kana (text TEXT PRIMARY KEY, entry_ids BLOB)")
        cursor.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB)")
        
        # Insert entries
        logger.info("Inserting entries...")
        cursor.execute("BEGIN TRANSACTION")
        for i, entry in enumerate(self.entries):
            cursor.execute("INSERT INTO entries (id, data) VALUES (?, ?)", (i, pickle.dumps(entry, protocol=pickle.HIGHEST_PROTOCOL)))
        
        # Insert lookups
        logger.info("Inserting lookups...")
        for text, ids in self.lookup_kan.items():
            cursor.execute("INSERT INTO lookup_kan (text, entry_ids) VALUES (?, ?)", (text, pickle.dumps(ids, protocol=pickle.HIGHEST_PROTOCOL)))
            
        for text, ids in self.lookup_kana.items():
            cursor.execute("INSERT INTO lookup_kana (text, entry_ids) VALUES (?, ?)", (text, pickle.dumps(ids, protocol=pickle.HIGHEST_PROTOCOL)))
            
        # Insert meta
        meta_data = {
            'deconjugator_rules': self.deconjugator_rules,
            'priority_map': self.priority_map,
            'frequency_map': self.frequency_map
        }
        for k, v in meta_data.items():
            cursor.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (k, pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)))
            
        conn.commit()
        conn.close()
        logger.info("SQLite conversion complete.")

    def load_dictionary(self, file_path: str) -> bool:
        if self._is_loaded:
            return True
            
        # Check for SQLite DB first
        db_path = file_path.replace('.pkl', '.db')
        if os.path.exists(db_path):
            return self.load_dictionary_sqlite(db_path)
            
        logger.info("Loading dictionary from file...")
        start_time = time.perf_counter()
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            if config.enable_jmdict:
                self.entries = data['entries']
                # Convert to CompactEntry if loaded from old pickle
                if self.entries and isinstance(self.entries[0], dict):
                    logger.info("Converting dictionary entries to compact format...")
                    new_entries = []
                    for e in self.entries:
                        new_entries.append(CompactEntry(
                            e['id'], e['kebs'], e['rebs'], e['senses'],
                            e.get('raw_k_ele', []), e.get('raw_r_ele', []), e.get('raw_sense', [])
                        ))
                    self.entries = new_entries
                    logger.info("Conversion complete.")

                self.lookup_kan = data['lookup_kan']
                self.lookup_kana = data['lookup_kana']
                
                # Auto-convert to SQLite for next time
                try:
                    self.deconjugator_rules = data['deconjugator_rules']
                    self.priority_map = data['priority_map']
                    self.frequency_map = data.get('frequency_map', {})
                    self.convert_to_sqlite(db_path)
                    logger.info("Automatically converted dictionary to SQLite for future performance.")
                except Exception as e:
                    logger.error(f"Failed to auto-convert to SQLite: {e}")
                    
            else:
                logger.info("JMDict disabled in settings. Skipping JMDict entries.")
                self.entries = []
                self.lookup_kan = defaultdict(list)
                self.lookup_kana = defaultdict(list)

            self.deconjugator_rules = data['deconjugator_rules']
            self.priority_map = data['priority_map']
            self.frequency_map = data.get('frequency_map', {})
            self._is_loaded = True
            duration = time.perf_counter() - start_time
            logger.info(f"Dictionary loaded in {duration:.2f} seconds.")
            return True
        except FileNotFoundError:
            script_extension = "bat" if IS_WINDOWS else "sh"
            logger.error(
                f"ERROR: Dictionary file '{file_path}' not found. Add the file or try running the build.dictonary.{script_extension} script in the repo.")
            return False
        except Exception as e:
            logger.error(f"ERROR: Failed to load dictionary from {file_path}: {e}")
            return False

    def load_dictionary_sqlite(self, db_path: str) -> bool:
        logger.info(f"Loading dictionary from SQLite DB: {db_path}")
        start_time = time.perf_counter()
        try:
            # We need to keep the connection open
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            
            if config.enable_jmdict:
                self.entries = SqliteEntryList(self.conn)
                self.lookup_kan = SqliteLookupMap(self.conn, "lookup_kan")
                self.lookup_kana = SqliteLookupMap(self.conn, "lookup_kana")
            else:
                self.entries = []
                self.lookup_kan = defaultdict(list)
                self.lookup_kana = defaultdict(list)
                
            # Load meta
            cursor = self.conn.cursor()
            cursor.execute("SELECT key, value FROM meta")
            meta = {row[0]: pickle.loads(row[1]) for row in cursor.fetchall()}
            
            self.deconjugator_rules = meta.get('deconjugator_rules', [])
            self.priority_map = meta.get('priority_map', {})
            self.frequency_map = meta.get('frequency_map', {})
            
            self._is_loaded = True
            duration = time.perf_counter() - start_time
            logger.info(f"SQLite Dictionary loaded in {duration:.2f} seconds.")
            return True
        except Exception as e:
            logger.error(f"Failed to load SQLite dictionary: {e}")
            return False
