extension = {
    "definitions": {
        "float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"},
        "url": {
            "pattern": (
                r"^https?://(www\.)?[a-z0-9.-]*\.[a-z]{2,}"
                r"([^<>%\x20\x00-\x1f\x7F]|%[0-9a-fA-F]{2})*$"
            )
        },
    },
    "additionalProperties": False,
}
