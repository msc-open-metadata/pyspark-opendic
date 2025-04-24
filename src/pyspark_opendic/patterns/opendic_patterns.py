import re


class OpenDicPatterns:
    # Syntax: CREATE [OR REPLACE] [TEMPORARY] OPEN <object_type> <name> [IF NOT EXISTS] [AS <alias>] [PROPS { <properties> }]
    @staticmethod
    def create():
        return (
            r"^create"  # "create" at the start
            r"(?:\s+or\s+replace)?"  # Optional "or replace"
            r"(?:\s+temporary)?"  # Optional "temporary"
            r"\s+open\s+(?P<object_type>\w+)"  # Required object type after "open"
            r"\s+(?P<name>\w+)"  # Required name of the object
            r"(?:\s+if\s+not\s+exists)?"  # Optional "if not exists"
            r"(?:\s+as\s+(?P<alias>\w+))?"  # Optional alias after "as"
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?"  # Optional "props" keyword, but curly braces are mandatory if present - This is a JSON object
        )

    # Syntax: SHOW OPEN TYPES
    @staticmethod
    def show_types():
        return (
            r"^show"  # "show" at the start
            r"\s+open\s+types$"  # Required "open types"
        )

    # Syntax: SHOW OPEN <object_type>[s]
    @staticmethod
    def show():
        return (
            r"^show"  # "show" at the start
            r"\s+open\s+(?P<object_type>(?!types$)\w+)"  # Required object type after "open" and not "TYPES"
            r"s?$"  # Optionally match a trailing "s"
        )

    # Syntax: SYNC OPEN <object_type> FOR <platform>
    @staticmethod
    def sync():
        return (
            r"^sync"  # "sync" at the start
            r"\s+open\s+(?P<object_type>\w+)\s+for"  # "open platforms for"
            r"\s+(?P<platform>\w+)"  # Object type (e.g., function)
            r"$"  # End of string
        )

    # Syntax: DEFINE OPEN <udoType> PROPS { <properties> }
    @staticmethod
    def define():
        return (
            r"^define"  # "DEFINE" at the start
            r"\s+open\s+(?P<udoType>\w+)"  # Required UDO type (e.g., "function")
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?"  # REQUIRED PROPS with JSON inside {}
            r"$"
        )

    # Syntax: DROP OPEN <object_type>
    @staticmethod
    def drop():
        return (
            r"^drop"  # "DROP" at the start
            r"\s+open\s+(?P<object_type>\w+)"  # Required object type after "open"
        )

    # Syntax: ADD OPEN MAPPING function PLATFORM <platform> SYNTAX {...} PROPS {...}
    @staticmethod
    def add_mapping():
        return (
            r"^add"
            r"\s+open\s+mapping"
            r"\s+(?P<object_type>\w+)"
            r"\s+platform\s+(?P<platform>\w+)"
            r"\s+syntax\s*\{\s*(?P<syntax>[\s\S]*?)\s*\}"
            r"\s+props\s*(?P<props>\{[\s\S]*?\})"
            r"$"
        )

    # Syntax: SHOW OPEN MAPPING <object_type> PLATFORM <platform>
    @staticmethod
    def show_mapping_for_object_and_platform():
        return (
            r"^show"  # "show" at the start
            r"\s+open\s+mapping"  # "open mapping"
            r"\s+(?P<object_type>\w+)"  # Object type (e.g., function)
            r"\s+platform\s+(?P<platform>\w+)"  # Platform name (e.g., snowflake)
            r"$"  # End of string
        )

    # Syntax: SHOW OPEN PLATFORMS FOR <object_type>
    @staticmethod
    def show_platforms_for_object():
        return (
            r"^show"  # "show" at the start
            r"\s+open\s+platforms\s+for"  # "open platforms for"
            r"\s+(?P<object_type>\w+)"  # Object type (e.g., function)
            r"$"  # End of string
        )

    # Syntax: SHOW OPEN PLATFORMS
    @staticmethod
    def show_platforms_all():
        return (
            r"^show"  # "show" at the start
            r"\s+open\s+platforms$"  # Required "open platforms"
            r"$"  # End of string
        )

    # Syntax: SHOW OPEN MAPPING[S] FOR <platform>
    @staticmethod
    def show_mappings_for_platform():
        return (
            r"^show"  # "show" at the start
            r"\s+open\s+mappings?\s+for\s+(?P<platform>\w+)"  # "open mappings" (with optional 's') followed by 'for' and platform name
            r"$"  # End of string
        )

    # Syntax: DROP OPEN MAPPING[S] FOR <platform>
    @staticmethod
    def drop_mapping_for_platform():
        return (
            r"^drop"  # "drop" at the start
            r"\s+open\s+mappings?\s+"  # "open mapping" with optional "s" for plural
            r"for\s+(?P<platform>\w+)"  # "for" followed by platform name
            r"$"  # End of string
        )

    # Syntax: ALTER OPEN <object_type> <name> [PROPS { <properties> }]
    @staticmethod
    def alter():
        return (
            r"^alter"
            r"\s+open\s+(?P<object_type>\w+)"  # Required object type after "open"
            r"\s+(?P<name>\w+)"  # Required name of the object
            r"(?:\s+props\s*(?P<properties>\{[\s\S]*\}))?"  # Optional props with JSON inside {}
            r"$"
        )

    # Compile all patterns up front with correct flags
    @staticmethod
    def compiled_patterns():
        return [
            ("create", re.compile(OpenDicPatterns.create(), re.IGNORECASE)),
            ("alter", re.compile(OpenDicPatterns.alter(), re.IGNORECASE)),
            ("show_types", re.compile(OpenDicPatterns.show_types(), re.IGNORECASE)),
            ("show_platforms_all", re.compile(OpenDicPatterns.show_platforms_all(), re.IGNORECASE)),
            ("show_mappings_for_platform", re.compile(OpenDicPatterns.show_mappings_for_platform(), re.IGNORECASE)),
            ("drop_mapping_for_platform", re.compile(OpenDicPatterns.drop_mapping_for_platform(), re.IGNORECASE)),
            ("show", re.compile(OpenDicPatterns.show(), re.IGNORECASE)),
            ("show_mapping_for_object_and_platform", re.compile(OpenDicPatterns.show_mapping_for_object_and_platform(), re.IGNORECASE)),
            ("show_platforms_for_object", re.compile(OpenDicPatterns.show_platforms_for_object(), re.IGNORECASE)),
            ("sync", re.compile(OpenDicPatterns.sync(), re.IGNORECASE)),
            ("define", re.compile(OpenDicPatterns.define(), re.IGNORECASE)),
            ("drop", re.compile(OpenDicPatterns.drop(), re.IGNORECASE)),
            ("add_mapping", re.compile(OpenDicPatterns.add_mapping(), re.IGNORECASE | re.DOTALL)),
        ]
