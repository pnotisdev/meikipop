import json
import zipfile
import os
import logging
import hashlib
import shutil
from typing import List, Dict, Any, Callable, Optional, Tuple
from pathlib import Path

try:
    from PIL import Image
    import io
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Ensure image cache directory exists
IMAGE_CACHE_DIR = os.path.join(os.getcwd(), 'data', 'images')
os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)

class YomichanImportError(Exception):
    pass

class YomichanConverter:
    def __init__(self, image_handler: Optional[Callable[[str], str]] = None):
        self.image_handler = image_handler

    def convert_entry(self, item: List, dict_title: str) -> Dict[str, Any]:
        """
        Converts a Yomichan term entry (array) to the internal dictionary format.
        Yomichan v3 format:
        [expression, reading, definition_tags, rules, score, glossary, sequence, term_tags]
        """
        try:
            if not isinstance(item, list):
                return None

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
                glossary = self._stringify_glossary(glossary)
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
            # Try to recover basic info if possible
            try:
                expression = item[0]
                reading = item[1]
                glossary = item[5] if len(item) > 5 else (item[2] if len(item) > 2 else []) # v1 might be different
                
                # Very basic fallback
                if not expression and not reading: return None
                
                if isinstance(glossary, list):
                    glossary = self._stringify_glossary(glossary)
                else:
                    glossary = [str(glossary)]

                entry_id = int(hashlib.sha256(f"{dict_title}:{expression}:{reading}".encode('utf-8')).hexdigest()[:8], 16)
                
                kebs = [expression] if expression else []
                rebs = [reading] if reading else []
                
                entry = {
                    'id': entry_id,
                    'kebs': kebs,
                    'rebs': rebs,
                    'senses': [{'glosses': glossary, 'pos': []}],
                    'raw_k_ele': [{'keb': expression, 'pri': []}] if expression else [],
                    'raw_r_ele': [{'reb': reading, 'restr': [], 'pri': []}] if reading else [],
                    'raw_sense': [{'misc': [], 'pos': [], 'gloss': glossary}],
                    'source': dict_title
                }
                return entry
            except Exception:
                return None
                
        except Exception as e:
            logger.debug(f"Error converting entry {item}: {e}")
            return None

    def _stringify_glossary(self, glossary: List[Any]) -> List[str]:
        stringified = []
        for item in glossary:
            if isinstance(item, str):
                stringified.append(item)
            elif isinstance(item, dict):
                if item.get('type') == 'structured-content':
                    stringified.extend(self._handle_structured_content(item))
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

    def _handle_structured_content(self, item: Dict[str, Any]) -> List[str]:
        content = item.get('content')
        html = self._convert_node_to_html(content)
        return [html]

    def _convert_node_to_html(self, node: Any) -> str:
        if isinstance(node, str):
            # Escape HTML characters
            return node.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\n', '<br>')
        
        if isinstance(node, list):
            return "".join(self._convert_node_to_html(x) for x in node)
        
        if isinstance(node, dict):
            tag = node.get('tag')
            content = node.get('content')
            data = node.get('data', {})
            
            if tag == 'rt':
                return "" # Skip ruby text
            
            # Handle graphic/image
            if tag == 'div' and data.get('content') == 'graphic':
                return self._handle_graphic_node(node)
            
            if tag == 'img':
                # Direct img tag support if present in JSON
                return self._handle_img_tag(node)

            inner_html = self._convert_node_to_html(content) if content else ""
            
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
                    style.append("list-style-type: none")
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

    def _handle_graphic_node(self, node: Dict[str, Any]) -> str:
        image_path = None
        
        # Try to find path in children (usually in an 'a' tag or 'img' tag)
        content = node.get('content')
        if isinstance(content, list):
            for child in content:
                if isinstance(child, dict):
                    if child.get('tag') == 'img':
                        image_path = child.get('data', {}).get('src') or child.get('path')
                    elif child.get('tag') == 'a':
                        image_path = child.get('data', {}).get('path')
                    
                    if image_path: break
        elif isinstance(content, dict):
             if content.get('tag') == 'img':
                 image_path = content.get('data', {}).get('src') or content.get('path')
             elif content.get('tag') == 'a':
                 image_path = content.get('data', {}).get('path')
        
        if image_path and self.image_handler:
            local_path = self.image_handler(image_path)
            if local_path:
                # Use file URI for local files
                file_uri = Path(local_path).as_uri()
                return f'<div style="margin: 5px 0;"><img src="{file_uri}" style="max-width: 100%; height: auto;"></div>'
        
        return ""

    def _handle_img_tag(self, node: Dict[str, Any]) -> str:
        image_path = node.get('data', {}).get('src') or node.get('path')
        if image_path and self.image_handler:
            local_path = self.image_handler(image_path)
            if local_path:
                file_uri = Path(local_path).as_uri()
                return f'<img src="{file_uri}" style="max-width: 100%; height: auto;">'
        return ""

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

def parse_yomichan_zip(zip_path: str) -> Tuple[List[Dict[str, Any]], Dict]:
    """
    Parses a Yomichan/Yomitan dictionary ZIP file and returns a list of entries
    formatted for the internal Dictionary class.
    """
    entries = []
    frequency_map = {}
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Check for index.json to verify it's a valid dictionary
            if 'index.json' not in z.namelist():
                logger.warning(f"Skipping {zip_path}: index.json not found")
                return [], {}

            with z.open('index.json') as f:
                index = json.load(f)
                title = index.get('title', 'Unknown Dictionary')
                logger.info(f"Importing Yomichan dictionary: {title}")

            # Setup image handler
            def image_handler(rel_path: str) -> str:
                try:
                    # Sanitize path
                    safe_name = hashlib.md5(f"{title}:{rel_path}".encode('utf-8')).hexdigest()
                    ext = os.path.splitext(rel_path)[1].lower()
                    if not ext: ext = '.png'
                    
                    target_ext = '.png' if ext == '.avif' else ext
                    target_filename = safe_name + target_ext
                    target_path = os.path.join(IMAGE_CACHE_DIR, target_filename)
                    
                    if os.path.exists(target_path):
                        return target_path
                    
                    # Extract
                    try:
                        with z.open(rel_path) as source:
                            image_data = source.read()
                    except KeyError:
                        # Try finding case-insensitive match or other issues
                        return ""

                    if ext == '.avif' and PIL_AVAILABLE:
                        try:
                            img = Image.open(io.BytesIO(image_data))
                            img.save(target_path, 'PNG')
                            return target_path
                        except Exception as e:
                            logger.warning(f"Failed to convert AVIF {rel_path}: {e}")
                            # Fallback
                            target_path_orig = os.path.join(IMAGE_CACHE_DIR, safe_name + ext)
                            with open(target_path_orig, 'wb') as f:
                                f.write(image_data)
                            return target_path_orig
                    else:
                        with open(target_path, 'wb') as f:
                            f.write(image_data)
                        return target_path
                        
                except Exception as e:
                    logger.error(f"Failed to extract image {rel_path}: {e}")
                    return ""

            converter = YomichanConverter(image_handler)

            # Iterate over term_bank files
            for filename in z.namelist():
                if filename.startswith('term_bank_') and filename.endswith('.json'):
                    with z.open(filename) as f:
                        term_bank = json.load(f)
                        for item in term_bank:
                            entry = converter.convert_entry(item, title)
                            if entry:
                                entries.append(entry)
                elif filename.startswith('term_meta_bank_') and filename.endswith('.json'):
                    with z.open(filename) as f:
                        meta_bank = json.load(f)
                        for item in meta_bank:
                            if isinstance(item, list) and len(item) >= 3 and item[1] == 'freq':
                                term = item[0]
                                data = item[2]
                                reading = ""
                                if isinstance(data, dict):
                                    reading = data.get('reading', "")
                                    freq_val = data
                                    if 'frequency' in data:
                                         freq_val = data['frequency']
                                    frequency_map[(term, reading)] = freq_val
                                
    except zipfile.BadZipFile:
        logger.error(f"Failed to read {zip_path}: Bad ZIP file")
    except Exception as e:
        logger.error(f"Error importing {zip_path}: {e}")

    return entries, frequency_map

def parse_yomichan_dir(dir_path: str) -> Tuple[List[Dict[str, Any]], Dict]:
    """
    Parses a Yomichan/Yomitan dictionary directory and returns a list of entries
    formatted for the internal Dictionary class.
    """
    entries = []
    frequency_map = {}
    
    try:
        # Check for index.json to verify it's a valid dictionary
        index_path = os.path.join(dir_path, 'index.json')
        if not os.path.exists(index_path):
            logger.warning(f"Skipping {dir_path}: index.json not found")
            return [], {}

        with open(index_path, 'r', encoding='utf-8') as f:
            index = json.load(f)
            title = index.get('title', 'Unknown Dictionary')
            logger.info(f"Importing Yomichan dictionary from dir: {title}")

        # Setup image handler
        def image_handler(rel_path: str) -> str:
            try:
                full_source_path = os.path.join(dir_path, rel_path)
                if not os.path.exists(full_source_path):
                    return ""
                
                # Sanitize path
                safe_name = hashlib.md5(f"{title}:{rel_path}".encode('utf-8')).hexdigest()
                ext = os.path.splitext(rel_path)[1].lower()
                if not ext: ext = '.png'
                
                target_ext = '.png' if ext == '.avif' else ext
                target_filename = safe_name + target_ext
                target_path = os.path.join(IMAGE_CACHE_DIR, target_filename)
                
                if os.path.exists(target_path):
                    return target_path
                
                if ext == '.avif' and PIL_AVAILABLE:
                    try:
                        img = Image.open(full_source_path)
                        img.save(target_path, 'PNG')
                        return target_path
                    except Exception as e:
                        logger.warning(f"Failed to convert AVIF {rel_path}: {e}")
                        # Fallback: copy
                        target_path_orig = os.path.join(IMAGE_CACHE_DIR, safe_name + ext)
                        shutil.copy2(full_source_path, target_path_orig)
                        return target_path_orig
                else:
                    shutil.copy2(full_source_path, target_path)
                    return target_path
                    
            except Exception as e:
                logger.error(f"Failed to process image {rel_path}: {e}")
                return ""

        converter = YomichanConverter(image_handler)

        # Iterate over term_bank files
        for filename in os.listdir(dir_path):
            if filename.startswith('term_bank_') and filename.endswith('.json'):
                file_path = os.path.join(dir_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    term_bank = json.load(f)
                    for item in term_bank:
                        entry = converter.convert_entry(item, title)
                        if entry:
                            entries.append(entry)
            elif filename.startswith('term_meta_bank_') and filename.endswith('.json'):
                file_path = os.path.join(dir_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    meta_bank = json.load(f)
                    for item in meta_bank:
                        if isinstance(item, list) and len(item) >= 3 and item[1] == 'freq':
                            term = item[0]
                            data = item[2]
                            reading = ""
                            if isinstance(data, dict):
                                reading = data.get('reading', "")
                                freq_val = data
                                if 'frequency' in data:
                                     freq_val = data['frequency']
                                frequency_map[(term, reading)] = freq_val
                                
    except Exception as e:
        logger.error(f"Error importing {dir_path}: {e}")

    return entries, frequency_map

# Keep _convert_yomichan_entry for backward compatibility if imported elsewhere, 
# but redirect to class
def _convert_yomichan_entry(item: List, dict_title: str) -> Dict[str, Any]:
    return YomichanConverter().convert_entry(item, dict_title)

def _convert_node_to_html(node: Any) -> str:
    return YomichanConverter()._convert_node_to_html(node)

