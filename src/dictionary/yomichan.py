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

def parse_yomichan_dir(dir_path: str) -> List[Dict[str, Any]]:
    """
    Parses a Yomichan/Yomitan dictionary directory and returns a list of entries
    formatted for the internal Dictionary class.
    """
    entries = []
    
    try:
        # Check for index.json to verify it's a valid dictionary
        index_path = os.path.join(dir_path, 'index.json')
        if not os.path.exists(index_path):
            logger.warning(f"Skipping {dir_path}: index.json not found")
            return []

        with open(index_path, 'r', encoding='utf-8') as f:
            index = json.load(f)
            title = index.get('title', 'Unknown Dictionary')
            logger.info(f"Importing Yomichan dictionary from dir: {title}")

        # Iterate over term_bank files
        for filename in os.listdir(dir_path):
            if filename.startswith('term_bank_') and filename.endswith('.json'):
                file_path = os.path.join(dir_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    term_bank = json.load(f)
                    for item in term_bank:
                        entry = _convert_yomichan_entry(item, title)
                        if entry:
                            entries.append(entry)
                                
    except Exception as e:
        logger.error(f"Error importing {dir_path}: {e}")

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

def _convert_node_to_html(node: Any) -> str:
    if isinstance(node, str):
        # Escape HTML characters
        return node.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\n', '<br>')
    
    if isinstance(node, list):
        return "".join(_convert_node_to_html(x) for x in node)
    
    if isinstance(node, dict):
        tag = node.get('tag')
        content = node.get('content')
        data = node.get('data', {})
        
        if tag == 'rt':
            return "" # Skip ruby text
            
        inner_html = _convert_node_to_html(content) if content else ""
        
        if tag == 'br':
            return "<br>"
        
        if tag == 'span':
            style = []
            sc_content = data.get('content')
            sc_class = data.get('class')
            
            if sc_class == 'tag':
                style.append("font-size: 0.8em")
                style.append("font-weight: bold")
                style.append("margin-right: 0.5em")
                style.append("padding: 0.2em 0.3em")
                style.append("vertical-align: text-bottom")
                style.append("border-radius: 3px")
                # style.append("border: 1px solid #ccc") # Removed border to match flat look better
            
            if sc_content == 'part-of-speech-info':
                style.append("background-color: #565656")
                style.append("color: white")
            elif sc_content == 'misc-info':
                style.append("background-color: brown")
                style.append("color: white")
            elif sc_content == 'field-info':
                style.append("background-color: purple")
                style.append("color: white")
            elif sc_content == 'dialect-info':
                style.append("background-color: green")
                style.append("color: white")
            elif sc_content == 'lang-source-wasei':
                style.append("background-color: orange")
                style.append("color: black")
            elif sc_content == 'example-keyword':
                 style.append("color: #00FF00")
            elif sc_content == 'reference-label':
                style.append("font-size: 0.8em")
                style.append("margin-right: 0.5rem")
                # Color depends on parent usually, but we can default to grey or specific if we knew context
                style.append("color: #888") 

            style_str = "; ".join(style)
            if style_str:
                return f'<span style="{style_str}">{inner_html}</span>'
            return f'<span>{inner_html}</span>'

        if tag == 'div':
            style = []
            sc_content = data.get('content')
            sc_class = data.get('class')
            
            if sc_class == 'extra-box':
                style.append("border-radius: 0.4rem")
                style.append("border-style: none none none solid")
                style.append("border-width: 3px")
                style.append("margin-bottom: 0.5rem")
                style.append("margin-top: 0.5rem")
                style.append("padding: 0.5rem")
                style.append("width: fit-content")

            if sc_content == 'example-sentence':
                style.append("margin-top: 0.5em")
                style.append("margin-bottom: 0.5em")
                style.append("padding-left: 0.5em")
                style.append("border-left: 3px solid gray")
            elif sc_content == 'info-gloss':
                style.append("border-color: green")
                style.append("background-color: rgba(0, 128, 0, 0.1)")
            elif sc_content == 'sense-note':
                style.append("border-color: goldenrod")
                style.append("background-color: rgba(218, 165, 32, 0.1)")
            elif sc_content == 'lang-source':
                style.append("border-color: purple")
                style.append("background-color: rgba(128, 0, 128, 0.1)")
            elif sc_content == 'xref':
                style.append("border-color: #1A73E8")
                style.append("background-color: rgba(26, 115, 232, 0.1)")
            elif sc_content == 'antonym':
                style.append("border-color: brown")
                style.append("background-color: rgba(165, 42, 42, 0.1)")
            
            style_str = "; ".join(style)
            if style_str:
                return f'<div style="{style_str}">{inner_html}</div>'
            return f'<div>{inner_html}</div>'

        if tag == 'ul':
            style = []
            sc_content = data.get('content')
            if sc_content == 'sense-groups':
                style.append("list-style-type: none") # We can't easily do "＊" in simple HTML/CSS without pseudo-elements which might not work well in Qt
            elif sc_content == 'glossary':
                style.append("list-style-type: disc")
                style.append("padding-left: 1.5em")
            
            style_str = "; ".join(style)
            if style_str:
                return f'<ul style="{style_str}">{inner_html}</ul>'
            return f'<ul>{inner_html}</ul>'
        
        if tag == 'ol':
            return f'<ol>{inner_html}</ol>'
            
        if tag == 'li':
            style = []
            sc_content = data.get('content')
            
            if sc_content == 'sense-group':
                style.append("margin-top: 0.1em")
            elif sc_content == 'forms':
                style.append("margin-top: 0.5em")
            
            style_str = "; ".join(style)
            if style_str:
                return f'<li style="{style_str}">{inner_html}</li>'
            return f'<li>{inner_html}</li>'
            
        if tag == 'ruby':
            return inner_html
        
        if tag == 'table':
            return f'<table border="1" style="border-collapse: collapse; margin-top: 0.2em;">{inner_html}</table>'
            
        if tag == 'tr':
            return f'<tr>{inner_html}</tr>'
            
        if tag == 'th':
            return f'<th style="border: 1px solid #555; padding: 2px; text-align: left; font-weight: normal;">{inner_html}</th>'
            
        if tag == 'td':
            return f'<td style="border: 1px solid #555; padding: 2px; text-align: center;">{inner_html}</td>'

        return inner_html

    return ""

def _handle_structured_content(item: Dict[str, Any]) -> List[str]:
    content = item.get('content')
    html = _convert_node_to_html(content)
    return [html]

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
