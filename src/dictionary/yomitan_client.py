
import requests
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from .customdict import DictionaryEntry
from . import structured_content

logger = logging.getLogger(__name__)

class YomitanClient:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip('/')
        self.enabled = False
        # Simple check if API is reachable (optional, or rely on config)
        
    def check_connection(self) -> bool:
        try:
            r = requests.post(f"{self.api_url}/yomitanVersion", timeout=1)
            return r.status_code == 200
        except:
            return False

    def lookup(self, term: str) -> List[DictionaryEntry]:
        """
        Fetch definitions from Yomitan API.
        Returns a list of DictionaryEntry objects.
        """
        entries = []
        try:
            response = requests.post(
                f"{self.api_url}/termEntries", 
                json={"term": term}, 
                timeout=2
            )
            
            if response.status_code != 200:
                logger.error(f"Yomitan API returned status {response.status_code}: {response.text}")
                return []
                
            data = response.json()
            # Response: { "dictionaryEntries": [ ... ], "originalTextLength": ... }
            
            raw_entries = data.get('dictionaryEntries', [])
            
            for idx, raw_entry in enumerate(raw_entries):
                entry = self._convert_api_entry(raw_entry, term, idx)
                if entry:
                    entries.append(entry)
                    
        except Exception as e:
            logger.error(f"Error querying Yomitan API: {e}")
            return []
            
        return entries

    def _convert_api_entry(self, item: Dict[str, Any], lookup_term: str, index: int) -> Optional[DictionaryEntry]:
        """
        Converts a single API dictionary entry object to DictionaryEntry.
        """
        # API entry structure:
        # {
        #   "headwords": [ { "term": "...", "reading": "...", ... } ],
        #   "definitions": [ { "content": ..., "dictionary": "..." } ],
        #   ...
        # }
        
        headwords = item.get('headwords', [])
        if not headwords:
            return None
            
        # Use first headword for main term/reading
        # Ideally we might split into multiple entries if multiple headwords?
        # But 'headwords' usually grouped by same sense.
        primary_headword = headwords[0]
        written_form = primary_headword.get('term', lookup_term)
        reading = primary_headword.get('reading', '')

        # Collect tags/frequencies from wrapper if available, or headword
        tags = set()
        # API structure for tags might be in 'tags' list of strings or objects
        # Looking at docs/examples: headwords have tags, definitions have tags.
        # Let's aggregate tags from headwords?
        for h in headwords:
            for t in h.get('tags', []):
                if isinstance(t, dict): 
                    tags.add(t.get('name', '')) # 'name' or 'content'? Example says 'content' for detailed tag object?
                    # wait, example: "tags": [ { "name": "priority...", "content": ["..."] } ]
                    # simple tags might serve just fine if present.
                    # Or 'wordClasses' -> "v5"
                elif isinstance(t, str):
                    tags.add(t)
            for wc in h.get('wordClasses', []):
                 tags.add(wc)

        frequency_tags = set()
        frequencies = item.get('frequencies', [])
        for f in frequencies:
            # f: { dictionary: "...", frequency: 123, ... }
            # Let's format as "Dict: 123"
            d_name = f.get('dictionaryAlias') or f.get('dictionary', '')
            val = f.get('displayValue') or f.get('frequency')
            if d_name and val:
                frequency_tags.add(f"{d_name}: {val}")

        # Senses
        senses = []
        definitions = item.get('definitions', [])
        for target_def in definitions:
            # target_def has 'entries' which contains the content?
            # Example: "definitions": [ { "dictionary": "...", "entries": [ { "type": "structured-content", "content": ... } ] } ]
            
            dict_name = target_def.get('dictionaryAlias') or target_def.get('dictionary', 'Unknown')
            
            # Glosses from 'entries'
            glosses = []
            def_entries = target_def.get('entries', [])
            for de in def_entries:
                if isinstance(de, dict) and de.get('type') == 'structured-content':
                     # Use shared renderer
                     html_list = structured_content.handle_structured_content(de)
                     glosses.extend(html_list)
                else:
                    # fallback
                    glosses.append(str(de))
            
            if glosses:
                senses.append({
                    'glosses': glosses,
                    'pos': [], # POS is often at headword level in Yomitan API result
                    'source': dict_name
                })
        
        if not senses:
            return None

        return DictionaryEntry(
            id=index, # arbitrary ID
            written_form=written_form,
            reading=reading,
            senses=senses,
            tags=tags,
            frequency_tags=frequency_tags,
            deconjugation_process=() # API doesn't return deinflection steps easily in this view (it does in "headwords.sources")
            # "sources": [ { "deinflectedText": "...", "transformedText": "..." } ]
            # We could extract it.
        )
