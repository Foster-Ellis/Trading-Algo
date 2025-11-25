# Trading News Pipeline & Semantic Processing

This project ingests real-time market news, normalizes it into a unified data format, deduplicates entries, and stores them in ClickHouse for further processing. A separate semantic inference module reads unprocessed news and assigns a sentiment vector (positive, neutral, negative). The system is modular, scalable, and structured for future integration into a full trading engine.

---

## How Data Ingestion Works

1. **WebSocket Stream**  
   Live news events are received through a WebSocket connection.

2. **Normalization**  
   Raw messages are converted into a standardized `NewsItem` structure.

3. **Deduplication**  
   A hash is created from key identifying fields.  
   If an item with the same hash already exists in ClickHouse, it is skipped.

4. **Storage**  
   Unique items are written to the `news` table with an empty `semantic` field.

5. **Semantic Processing (Separate Runtime)**  
   A second process monitors ClickHouse for rows missing semantic values,  
   runs inference, and updates the database asynchronously.

---

## Project File Structure

### `pipeline/`  (Python 3.14)
- **pipeline_controller.py** – Main orchestrator for WebSocket ingestion and writing to ClickHouse.  
- **ingestion/** – Handles external news sources.  
  - `alpaca_news_ws.py` – Alpaca news WebSocket handler.  
- **processing/** – Data manipulation and utility functions.  
  - `hasher.py` – Generates deduplication hashes.  
  - `dataframe_builder.py` – Converts NewsItem objects into row dictionaries.  
- **storage/** – ClickHouse connectivity and I/O.  
  - `clickhouse_client.py`  
  - `clickhouse_writer.py`  
- **models/** – Pydantic schemas for unified structured data.  
  - `news_item.py`  
- **data/** – Any stored dumps or test samples, such as `news_dump.jsonl`.  
- `pyproject.toml`, `uv.lock` – Environment definition for the ingestion runtime.

### `ml_semantic/`  (Python 3.12 or 3.13)
- **semantic_worker.py** – Background inference processor.  
- **finbert_model.py** – Loads and runs the sentiment model.  
- `pyproject.toml`, `uv.lock` – Environment definition for the ML runtime.

---

Both directories operate independently and communicate only through ClickHouse, allowing flexible scaling and clean separation of concerns.
