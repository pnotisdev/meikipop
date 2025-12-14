# customdict.py
import json
import logging
import pickle
import time
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Set

from src.config.config import IS_WINDOWS
from src.dictionary.yomichan import parse_yomichan_zip

logger = logging.getLogger(__name__) # Get the logger

@dataclass
class DictionaryEntry:
    id: int
    written_form: str
    reading: str
    senses: list
    tags: Set[str]
    frequency_tags: Set[str]
    deconjugation_process: tuple
    priority: float = 0.0

class Dictionary:
    def __init__(self):
        self.entries = []
        self.lookup_kan = defaultdict(list)
        self.lookup_kana = defaultdict(list)
        self.deconjugator_rules = []
        self.priority_map = {}
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
        """Imports all Yomichan/Yomitan dictionaries (.zip) from a directory."""
        if not os.path.exists(directory_path):
            logger.warning(f"Yomichan dictionary directory not found: {directory_path}")
            return

        logger.info(f"Scanning for Yomichan dictionaries in: {directory_path}")
        for filename in os.listdir(directory_path):
            if filename.lower().endswith('.zip'):
                full_path = os.path.join(directory_path, filename)
                self.import_yomichan_zip(full_path)

    def import_yomichan_zip(self, zip_path: str):
        """Imports a single Yomichan/Yomitan dictionary ZIP."""
        new_entries = parse_yomichan_zip(zip_path)
        if not new_entries:
            return

        start_index = len(self.entries)
        self.entries.extend(new_entries)
        
        # Update lookups
        for i, entry in enumerate(new_entries):
            real_index = start_index + i
            for keb in entry['kebs']:
                self.lookup_kan[keb].append(real_index)
            for reb in entry['rebs']:
                self.lookup_kana[reb].append(real_index)
        
        logger.info(f"Imported {len(new_entries)} entries from {os.path.basename(zip_path)}")

    def save_dictionary(self, file_path: str):
        data_to_save = {'entries': self.entries, 'lookup_kan': self.lookup_kan, 'lookup_kana': self.lookup_kana, 'deconjugator_rules': self.deconjugator_rules, 'priority_map': self.priority_map}
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
            self.entries = data['entries']
            self.lookup_kan = data['lookup_kan']
            self.lookup_kana = data['lookup_kana']
            self.deconjugator_rules = data['deconjugator_rules']
            self.priority_map = data['priority_map']
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