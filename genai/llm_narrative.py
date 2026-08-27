"""
Turns a detected anomaly into a plain-English narrative.

Tries a local Ollama instance first (free, runs on your own machine).
If Ollama isn't running or the call fails for any reason, falls back to
a simple template-based sentence instead of crashing the pipeline —
this graceful degradation is a real AIOps design pattern worth
mentioning in interviews: the pipeline should never go down just
because an optional enrichment step is unavailable.

To actually use the LLM path: install Ollama on Windows from
https://ollama.com, run `ollama pull llama3.1`, then `ollama serve`
(or just leave the Ollama app running — it serves automatically).
"""

import os
import requests

# host.docker.internal lets a container reach services running on your
# actual Windows machine (like Ollama) — localhost inside the container
# would only mean "inside this container," not your laptop.
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")


def _fallback_narrative(title: str, anomaly_type: str, magnitude_pct, from_price, to_price) -> str:
    """Simple, deterministic narrative — used when the LLM is unavailable."""
    if anomaly_type == "STOCKOUT":
        return f"'{title}' went out of stock (was £{from_price}, previously in stock)."
    direction = "dropped" if magnitude_pct < 0 else "rose"
    return (
        f"'{title}' price {direction} {abs(magnitude_pct):.1f}% "
        f"(£{from_price} -> £{to_price})."
    )


def generate_narrative(title: str, anomaly_type: str, magnitude_pct, from_price, to_price) -> str:
    """
    Returns a one-to-two-sentence plain-English explanation of the anomaly.
    Tries the local LLM first; falls back to a template on any failure.
    """
    prompt = (
        f"You are a retail pricing analyst. In ONE short sentence, explain this "
        f"pricing anomaly for a shopper reading a dashboard. Be factual, no fluff.\n\n"
        f"Product: {title}\n"
        f"Anomaly type: {anomaly_type}\n"
        f"Price change: £{from_price} -> £{to_price} ({magnitude_pct}% change)\n"
    )

    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=8,
        )
        response.raise_for_status()
        text = response.json().get("response", "").strip()
        if text:
            return text
    except Exception as exc:
        print(f"[llm_narrative] Ollama unavailable ({exc}), using fallback template.")

    return _fallback_narrative(title, anomaly_type, magnitude_pct, from_price, to_price)