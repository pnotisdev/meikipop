# customdict.py
import json
import logging
import pickle
import time
import os
import concurrent.futures
from collections import defaultdict

from src.config.config import IS_WINDOWS, config
from src.dictionary.yomichan import parse_yomichan_zip, parse_yomichan_dir

logger = logging.getLogger(__name__) # Get the logger

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
            entry = {'id': entry_data['seq'], 'kebs': kebs, 'rebs': rebs, 'senses': senses_processed, 'raw_k_ele': entry_data.get('k_ele', []), 'raw_r_ele': entry_data.get('r_ele', []), 'raw_sense': entry_data.get('sense', [])}
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
        self.entries.extend(new_entries)
        
        # Update lookups
        for i, entry in enumerate(new_entries):
            real_index = start_index + i
            for keb in entry['kebs']:
                self.lookup_kan[keb].append(real_index)
            for reb in entry['rebs']:
                self.lookup_kana[reb].append(real_index)
        
        if frequency_map:
            self.frequency_map.update(frequency_map)
        
        logger.info(f"Imported {len(new_entries)} entries and {len(frequency_map)} frequency items from {source_name}")

    def save_dictionary(self, file_path: str):
        data_to_save = {'entries': self.entries, 'lookup_kan': self.lookup_kan, 'lookup_kana': self.lookup_kana, 'deconjugator_rules': self.deconjugator_rules, 'priority_map': self.priority_map, 'frequency_map': self.frequency_map}
        with open(file_path, 'wb') as f:
            pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load_dictionary(self, file_path: str) -> bool:
        if self._is_loaded:
            return True
        logger.info("Loading dictionary from file...")
        start_time = time.perf_counter()
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            if config.enable_jmdict:
                self.entries = data['entries']
                self.lookup_kan = data['lookup_kan']
                self.lookup_kana = data['lookup_kana']
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