import os
import json
import logging
from pathlib import Path
from step3_llm_filter import check_relevance

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Eval")

OUTPUT_DIR = Path("output")

def extract_text_from_json(json_data: dict) -> str:
    """Extracts a flat text representation from the JSON doc for LLM evaluation."""
    text_parts = []
    text_parts.append(json_data.get("topic", ""))
    for sec in json_data.get("sections", []):
        text_parts.append(sec.get("section_title", ""))
        for chap in sec.get("chapters", []):
            text_parts.append(chap.get("chapter_title", ""))
            for item in chap.get("items", []):
                text_parts.append(item.get("item_title", ""))
                text_parts.append(item.get("description", ""))
                for sum_item in item.get("sub_items", []):
                    text_parts.append(sum_item.get("sub_title", ""))
                    text_parts.append(sum_item.get("sub_description", ""))
    return "\n".join(filter(None, text_parts))

def main():
    json_files = list(OUTPUT_DIR.glob("*.json"))
    if not json_files:
        logger.info("❌ No JSON files found in output directory.")
        return

    logger.info(f"🔍 Found {len(json_files)} JSON files. Evaluating relevance...")
    logger.info("=" * 60)

    for i, file_path in enumerate(sorted(json_files), 1):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Extract plain text from JSON up to ~4000 chars
            text = extract_text_from_json(data)[:4000]
            
            # Use the existing check_relevance function which calls Gemini
            result = check_relevance(text)

            is_rel = result.get('is_relevant')
            score = result.get('confidence_score', 0)
            reason = result.get('reason', '')
            
            status = '✅ PASS' if is_rel else '❌ FAIL'
            logger.info(f"[{i}/{len(json_files)}] {file_path.name}")
            logger.info(f"  → Status: {status} (Score: {score})")
            logger.info(f"  → Reason: {reason}")
            logger.info("-" * 40)
            
        except Exception as e:
            logger.error(f"Error evaluating {file_path.name}: {e}")

if __name__ == "__main__":
    main()
