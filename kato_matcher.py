import psycopg2
from config import DB_CONFIG
from typing import Optional, Dict, List


class KatoMatcher:
    """
    Hierarchical KATO code matcher with caching and normalization.
    """

    def __init__(self):
        self.kato_tree = {}
        self.kato_by_code = {}
        self._load_kato_data()

    def _normalize_name(self, name: str) -> str:
        """
        Normalize KATO name by:
        - Converting to lowercase
        - Removing extra whitespace
        - Expanding abbreviations: р-н → район, обл. → область, г. → город
        """
        name = name.lower().strip()

        # Split by words and expand abbreviations for each word
        words = name.split()
        normalized_words = []

        abbreviations = {
            'р-н': 'район',
            'р-на': 'района',
            'р-ну': 'району',
            'р.': 'район',
            'обл.': 'область',
            'обл': 'область',
            'г.': 'город',
            'г': 'город',
            'с.': 'село',
            'с': 'село',
            'п.': 'поселок',
            'п': 'поселок',
            'а.': 'аул',
            'а': 'аул',
            'кент': 'кент',
            'ауыл': 'село'
        }

        for word in words:
            # Check if this word is an abbreviation
            if word in abbreviations:
                normalized_words.append(abbreviations[word])
            else:
                normalized_words.append(word)

        # Join back and remove extra spaces
        name = ' '.join(normalized_words)

        return name

    def _load_kato_data(self):
        """
        Load KATO data from database and build a hierarchical tree structure.
        Tree structure: {parent_code: {normalized_name: {code, name, children}}}
        """
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        try:
            query = """
            SELECT code, name_ru, parent_code, level
            FROM katoCode
            ORDER BY level, code
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            # Build flat lookup
            for code, name_ru, parent_code, level in rows:
                normalized_name = self._normalize_name(name_ru)
                self.kato_by_code[code] = {
                    'code': code,
                    'name_ru': name_ru,
                    'normalized_name': normalized_name,
                    'parent_code': parent_code,
                    'level': level,
                    'children': {}
                }

            # Build tree structure
            for code, data in self.kato_by_code.items():
                parent_code = data['parent_code']
                if parent_code and parent_code in self.kato_by_code:
                    parent = self.kato_by_code[parent_code]
                    parent['children'][data['normalized_name']] = data
                elif not parent_code or parent_code == '':
                    # Root level
                    self.kato_tree[data['normalized_name']] = data

            print(f"Loaded {len(self.kato_by_code)} KATO codes")

        finally:
            cursor.close()
            conn.close()

    def find_kato_code(self, kato_names: List[str]) -> Optional[str]:
        """
        Find KATO code by hierarchical matching.

        Args:
            kato_names: List of location names from general to specific
                       e.g., ["Республика Казахстан", "Кызылординская область", "Сырдарьинский р-н"]

        Returns:
            KATO code of the most specific (leaf) location, or None
        """
        if not kato_names:
            return None

        # Name aliases for renamed cities (Нур-Султан -> Астана)
        name_aliases = {
            'нур-султан': 'астана',
            'нұр-сұлтан': 'астана'
        }

        # Skip "Республика Казахстан" (level 0)
        filtered_names = []
        for name in kato_names:
            normalized = self._normalize_name(name)
            if 'республика казахстан' not in normalized:
                # Apply aliases
                for old_name, new_name in name_aliases.items():
                    if old_name in normalized:
                        normalized = normalized.replace(old_name, new_name)
                filtered_names.append(normalized)

        if not filtered_names:
            return None

        # Start from root and traverse down the tree
        current_nodes = self.kato_tree
        matched_code = None

        for name in filtered_names:
            # Try to find match in current level
            found = False
            for node_name, node_data in current_nodes.items():
                # Exact match or partial match
                if name in node_name or node_name in name:
                    matched_code = node_data['code']
                    current_nodes = node_data['children']
                    found = True
                    break

            if not found:
                # Try fuzzy matching - check if any words match
                name_words = set(name.split())
                for node_name, node_data in current_nodes.items():
                    node_words = set(node_name.split())
                    # If at least 1 significant word matches
                    common_words = name_words & node_words
                    if common_words:
                        matched_code = node_data['code']
                        current_nodes = node_data['children']
                        found = True
                        break

            if not found:
                # Fallback: search globally for the most specific name (last in list)
                if name == filtered_names[-1]:
                    # Search for this name across all KATO codes
                    for code, data in self.kato_by_code.items():
                        if name in data['normalized_name'] or data['normalized_name'] in name:
                            matched_code = data['code']
                            return matched_code
                # No match found, return last matched code or continue
                if matched_code:
                    break

        return matched_code

    def find_by_parent(self, parent_code: str, name: str) -> Optional[str]:
        """
        Find KATO code by parent code and name.

        Args:
            parent_code: Parent KATO code
            name: Location name to search

        Returns:
            KATO code or None
        """
        if parent_code not in self.kato_by_code:
            return None

        normalized_name = self._normalize_name(name)
        parent_node = self.kato_by_code[parent_code]

        for child_name, child_data in parent_node['children'].items():
            if normalized_name in child_name or child_name in normalized_name:
                return child_data['code']

        return None
