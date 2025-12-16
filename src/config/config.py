# src/config/config.py
import configparser
import logging
import sys

logger = logging.getLogger(__name__)

APP_NAME = "meikipop"
APP_VERSION = "v.1.5.4"
MAX_DICT_ENTRIES = 10
IS_LINUX = sys.platform.startswith('linux')
IS_WINDOWS = sys.platform.startswith('win')
IS_MACOS = sys.platform.startswith('darwin')

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        self._load()

    def _load(self):
        config = configparser.ConfigParser()

        # Step 1: Set hardcoded defaults
        defaults = {
            'Settings': {
                'hotkey': 'shift',
                'scan_region': 'region',
                'max_lookup_length': '25',
                'quality_mode': 'balanced',
                'ocr_provider': 'Google Lens',
                'auto_scan_mode': 'false',
                'auto_scan_mode_lookups_without_hotkey': 'true',
                'auto_scan_interval_seconds': '0.0',
                'magpie_compatibility': 'false',
                'extra_dictionaries_dir': 'user_dictionaries',
                'enabled_dictionaries': '',
                'enable_jmdict': 'true',
                'max_popup_width': '500',
                'max_popup_height': '400'
            },
            'Theme': {
                'theme_name': 'Nazeka',
                'font_family': '',
                'font_size_definitions': '14',
                'font_size_header': '18',
                'compact_mode': 'true',
                'show_deconjugation': 'false',
                'show_pos': 'false',
                'show_tags': 'false',
                'show_frequency': 'false',
                'color_background': '#2E2E2E',
                'color_foreground': '#F0F0F0',
                'color_highlight_word': '#88D8FF',
                'color_highlight_reading': '#90EE90',
                'background_opacity': '245',
                'popup_position_mode': 'flip_vertically',
                'border_color': '#555555',
                'border_radius': '8',
                'border_width': '1'
            },
            'Anki': {
                'deck_name': 'Default',
                'model_name': 'Meikipop Card',
                'url': 'http://127.0.0.1:8765',
                'show_hover_status': 'false'
            }
        }
        config.read_dict(defaults)

        # Step 2: Load from config.ini, creating it if it doesn't exist
        try:
            if not config.read('config.ini', encoding='utf-8'):
                with open('config.ini', 'w', encoding='utf-8') as configfile:
                    config.write(configfile)
                logger.info("config.ini not found, created with default settings.")
            else:
                logger.info("Loaded settings from config.ini.")
        except configparser.Error as e:
            logger.warning(f"Warning: Could not parse config.ini. Using defaults. Error: {e}")

        # Apply settings from the config object first
        self.hotkey = config.get('Settings', 'hotkey')
        self.scan_region = config.get('Settings', 'scan_region')
        self.max_lookup_length = config.getint('Settings', 'max_lookup_length')
        self.quality_mode = config.get('Settings', 'quality_mode')
        self.ocr_provider = config.get('Settings', 'ocr_provider')
        self.auto_scan_mode = config.getboolean('Settings', 'auto_scan_mode')
        self.auto_scan_mode_lookups_without_hotkey = config.getboolean('Settings',
                                                                       'auto_scan_mode_lookups_without_hotkey')
        self.auto_scan_interval_seconds = config.getfloat('Settings', 'auto_scan_interval_seconds')
        self.magpie_compatibility = config.getboolean('Settings', 'magpie_compatibility')
        self.extra_dictionaries_dir = config.get('Settings', 'extra_dictionaries_dir', fallback='user_dictionaries')
        self.enable_jmdict = config.getboolean('Settings', 'enable_jmdict', fallback=True)
        self.max_popup_width = config.getint('Settings', 'max_popup_width', fallback=500)
        self.max_popup_height = config.getint('Settings', 'max_popup_height', fallback=400)
        
        enabled_dicts_str = config.get('Settings', 'enabled_dictionaries', fallback=None)
        if enabled_dicts_str is None:
            self.enabled_dictionaries = None
        else:
            self.enabled_dictionaries = [d.strip() for d in enabled_dicts_str.split(',')] if enabled_dicts_str else []

        self.theme_name = config.get('Theme', 'theme_name')
        self.font_family = config.get('Theme', 'font_family')
        self.font_size_definitions = config.getint('Theme', 'font_size_definitions')
        self.font_size_header = config.getint('Theme', 'font_size_header')
        self.compact_mode = config.getboolean('Theme', 'compact_mode')
        self.show_deconjugation = config.getboolean('Theme', 'show_deconjugation')
        self.show_pos = config.getboolean('Theme', 'show_pos')
        self.show_tags = config.getboolean('Theme', 'show_tags')
        self.show_frequency = config.getboolean('Theme', 'show_frequency', fallback=False)
        self.color_background = config.get('Theme', 'color_background')
        self.color_foreground = config.get('Theme', 'color_foreground')
        self.color_highlight_word = config.get('Theme', 'color_highlight_word')
        self.color_highlight_reading = config.get('Theme', 'color_highlight_reading')
        self.background_opacity = config.getint('Theme', 'background_opacity')
        self.popup_position_mode = config.get('Theme', 'popup_position_mode')
        self.border_color = config.get('Theme', 'border_color', fallback='#555555')
        self.border_radius = config.getint('Theme', 'border_radius', fallback=8)
        self.border_width = config.getint('Theme', 'border_width', fallback=1)

        self.anki_deck_name = config.get('Anki', 'deck_name', fallback='Default')
        self.anki_model_name = config.get('Anki', 'model_name', fallback='Basic')
        self.anki_url = config.get('Anki', 'url', fallback='http://127.0.0.1:8765')
        self.anki_show_hover_status = config.getboolean('Anki', 'show_hover_status', fallback=False)

        self.is_enabled = True

        # todo command line args parsing

    def save(self):
        config = configparser.ConfigParser()
        config['Settings'] = {
            'hotkey': self.hotkey,
            'scan_region': self.scan_region,
            'max_lookup_length': str(self.max_lookup_length),
            'quality_mode': self.quality_mode,
            'ocr_provider': self.ocr_provider,
            'auto_scan_mode': str(self.auto_scan_mode).lower(),
            'auto_scan_mode_lookups_without_hotkey': str(self.auto_scan_mode_lookups_without_hotkey).lower(),
            'auto_scan_interval_seconds': str(self.auto_scan_interval_seconds),
            'extra_dictionaries_dir': self.extra_dictionaries_dir,
            'enabled_dictionaries': ','.join(self.enabled_dictionaries) if self.enabled_dictionaries else '',
            'enable_jmdict': str(self.enable_jmdict).lower(),
            'max_popup_width': str(self.max_popup_width),
            'max_popup_height': str(self.max_popup_height),
            'magpie_compatibility': str(self.magpie_compatibility).lower()
        }
        config['Theme'] = {
            'theme_name': self.theme_name,
            'font_family': self.font_family,
            'font_size_definitions': str(self.font_size_definitions),
            'font_size_header': str(self.font_size_header),
            'compact_mode': str(self.compact_mode).lower(),
            'show_deconjugation': str(self.show_deconjugation).lower(),
            'show_pos': str(self.show_pos).lower(),
            'show_tags': str(self.show_tags).lower(),
            'show_frequency': str(self.show_frequency).lower(),
            'color_background': self.color_background,
            'color_foreground': self.color_foreground,
            'color_highlight_word': self.color_highlight_word,
            'border_color': self.border_color,
            'border_radius': str(self.border_radius),
            'border_width': str(self.border_width),
            'color_highlight_reading': self.color_highlight_reading,
            'background_opacity': str(self.background_opacity),
            'popup_position_mode': self.popup_position_mode
        }
        config['Anki'] = {
            'deck_name': self.anki_deck_name,
            'model_name': self.anki_model_name,
            'url': self.anki_url,
            'show_hover_status': str(self.anki_show_hover_status).lower()
        }
        with open('config.ini', 'w', encoding='utf-8') as configfile:
            config.write(configfile)
        logger.info("Settings saved to config.ini.")

# The singleton instance
config = Config()