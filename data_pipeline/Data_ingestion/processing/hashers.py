import hashlib
import json


# ==========================================
# Alpaca News Hasher
# ==========================================
class AlpacaNewsHasher:
    @staticmethod
    def compute(message: dict) -> str:
        """
        Deterministic SHA256 hash for deduping Alpaca news items.

        Alpaca provides a stable 'id' field per news article.
        That is guaranteed unique and is the safest dedupe key.

        If for any reason 'id' is missing (rare schema change),
        we gracefully fall back to headline + created_at + url.
        """

        # Preferred: Alpaca's unique news ID
        alpaca_id = message.get("id")
        if alpaca_id:
            key = f"alpaca:{alpaca_id}"
        else:
            # Rare fallback — only used if Alpaca changes their schema someday
            content_fields = {
                "headline": message.get("headline", ""),
                "created_at": message.get("created_at", ""),
                "url": message.get("url", ""),
            }
            key = json.dumps(content_fields, sort_keys=True, separators=(",", ":"))

        return hashlib.sha256(key.encode("utf-8")).hexdigest()
