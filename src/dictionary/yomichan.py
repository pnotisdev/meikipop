import json
import zipfile
import os
import logging
import hashlib
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class YomichanImportError(Exception):
    pass

def parse_yomichan_zip(zip_path: str) -> List[Dict[str, Any]]:
    """
    Parses a Yomichan/Yomitan dictionary ZIP file and returns a list of entries
    formatted for the internal Dictionary class.
    """
    entries = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Check for index.json to verify it's a valid dictionary
            if 'index.json' not in z.namelist():
                logger.warning(f"Skipping {zip_path}: index.json not found")
                return []

            with z.open('index.json') as f:
                index = json.load(f)
                title = index.get('title', 'Unknown Dictionary')
                logger.info(f"Importing Yomichan dictionary: {title}")

            # Iterate over term_bank files
            for filename in z.namelist():
                if filename.startswith('term_bank_') and filename.endswith('.json'):
                    with z.open(filename) as f:
                        term_bank = json.load(f)
                        for item in term_bank:
                            entry = _convert_yomichan_entry(item, title)
                            if entry:
                                entries.append(entry)
                                
    except zipfile.BadZipFile:
        logger.error(f"Failed to read {zip_path}: Bad ZIP file")
    except Exception as e:
        logger.error(f"Error importing {zip_path}: {e}")

    return entries

def _extract_text_recursive(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, list):
        return "".join(_extract_text_recursive(x) for x in item)
    if isinstance(item, dict):
        if item.get('tag') == 'rt':
            return "" # Skip ruby text
        if 'content' in item:
            return _extract_text_recursive(item['content'])
    return ""

def _handle_structured_content(item: Dict[str, Any]) -> List[str]:
    lines = []
    content = item.get('content')
    
    if isinstance(content, list):
        for node in content:
            if isinstance(node, dict):
                tag = node.get('tag')
                if tag in ('ol', 'ul'):
                    list_items = node.get('content', [])
                    if isinstance(list_items, list):
                        for li in list_items:
                            text = _extract_text_recursive(li)
                            if text:
                                lines.append(text)
                else:
                    text = _extract_text_recursive(node)
                    if text:
                        lines.append(text)
            elif isinstance(node, str):
                lines.append(node)
    elif isinstance(content, str):
        lines.append(content)
    elif isinstance(content, dict):
         lines.append(_extract_text_recursive(content))
        
    return lines

def _stringify_glossary(glossary: List[Any]) -> List[str]:
    stringified = []
    for item in glossary:
        if isinstance(item, str):
            stringified.append(item)
        elif isinstance(item, dict):
            if item.get('type') == 'structured-content':
                stringified.extend(_handle_structured_content(item))
            else:
                # Fallback for unknown structure
                text = _extract_text_recursive(item)
                if text:
                    stringified.append(text)
                else:
                    stringified.append(str(item))
        else:
            stringified.append(str(item))
    return stringified

def _convert_yomichan_entry(item: List, dict_title: str) -> Dict[str, Any]:
    """
    Converts a Yomichan term entry (array) to the internal dictionary format.
    Yomichan v3 format:
    [expression, reading, definition_tags, rules, score, glossary, sequence, term_tags]
    """
    try:
        expression = item[0]
        reading = item[1]
        definition_tags = item[2]
        rules = item[3]
        score = item[4]
        glossary = item[5]
        sequence = item[6]
        term_tags = item[7]

        # Basic validation
        if not expression and not reading:
            return None
            
        # Normalize glossary
        if isinstance(glossary, list):
            glossary = _stringify_glossary(glossary)
        else:
            glossary = [str(glossary)]

        # Ensure ID is int; fallback to hash
        try:
            entry_id = int(sequence)
        except (ValueError, TypeError):
            entry_id = int(hashlib.sha256(f"{dict_title}:{expression}:{reading}".encode('utf-8')).hexdigest()[:8], 16)

        # Map to internal structure
        kebs = [expression] if expression else []
        rebs = [reading] if reading else []
        
        if not rebs and not kebs:
             return None

        pos = definition_tags.split(' ') if definition_tags else []
        
        senses = [{
            'glosses': glossary,
            'pos': pos
        }]

        # Construct raw elements for Lookup compatibility
        raw_k_ele = [{'keb': expression, 'pri': []}] if expression else []
        raw_r_ele = [{'reb': reading, 'restr': [], 'pri': []}] if reading else []

        raw_sense = [{
            'misc': [],
            'pos': pos,
            'gloss': glossary
        }]

        if term_tags:
            raw_sense[0]['misc'].extend(term_tags.split(' '))

        entry = {
            'id': entry_id,
            'kebs': kebs, #kanji
            'rebs': rebs, #readings
            'senses': senses,
            'raw_k_ele': raw_k_ele,
            'raw_r_ele': raw_r_ele,
            'raw_sense': raw_sense,
            'source': dict_title
        }
        
        return entry

    except IndexError:
        # Handle cases where the array is shorter than expected (older formats?)
        return None
    except Exception as e:
        logger.debug(f"Error converting entry {item}: {e}")
        return None
