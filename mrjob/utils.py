import json

def generate_stats_report(results, output_path, top_n=5):
    """Tworzy raport z top/min wartościami. Obsługuje też słowniki jako wartości."""
    report = {}

    if isinstance(results[0][1], (int, float)):
        # Proste wartości liczbowe
        sorted_results = sorted(results, key=lambda x: x[1], reverse=True)
        report["top"] = sorted_results[:top_n]
        report["min"] = sorted_results[-top_n:]
    elif isinstance(results[0][1], dict):
        # Dla każdego klucza w słowniku – osobny ranking
        keys = results[0][1].keys()
        for k in keys:
            ranked = sorted(
                results,
                key=lambda x: x[1].get(k, 0),
                reverse=True
            )
            report[k] = {
                "top": ranked[:top_n],
                "min": ranked[-top_n:]
            }
    else:
        print("[RAPORT] ⚠️ Nieznany typ danych, pomijam raport.")
        return

    # Zapis do pliku
    report_path = output_path.replace(".json", "_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)

    print(f"[RAPORT] ✅ Zapisano raport statystyk do: {report_path}")

