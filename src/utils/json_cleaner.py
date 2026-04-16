#Conversion de NDJSON vers JSON :
import json

input_path = "machine_learning_papers_optimal.jsonl"          # NDJSON
output_path = "papers_array.json"   # JSON tableau

data = []
with open(input_path, "r", encoding="utf-8") as f:
    for line_number, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            data.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"Ligne {line_number} invalide, ignorée: {e}")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Conversion terminée: {len(data)} objets écrits dans {output_path}")