import requests
import json

# Specify the search term
query = '"generative ai"'

# Define the API endpoint URL
url = "http://api.semanticscholar.org/graph/v1/paper/search/bulk"

# Define the query parameters
query_params = {
    "query": '"retrieval augmented generation"',
    "fields": "title,url,publicationTypes,publicationDate,abstract,authors",
    "year": "1950-"
}

# Directly define the API key (Reminder: Securely handle API keys in production environments)
api_key = "2y5uBuUCdd49YFT5Jsuhv5LGG2Wfh5CJaLIZfFz5"  # Replace with the actual API key

# Define headers with API key
headers = {"x-api-key": api_key}

# Send the API request
r = requests.get(url, params=query_params, headers=headers)
print("status:", r.status_code)
print("url:", r.url)
print("raw:", r.text[:1000])  # first 1000 chars
response = r.json()
print("keys:", list(response.keys()))

print("total:", response.get("total", "N/A"))

print(f"Will retrieve an estimated {response["total"]} documents")
retrieved = 0

seen_papers = set()
seen_tokens = set()
# Write results to json file and get next batch of results
with open(f"transformers_papers.json", "a") as file:
    while True:
        if r.status_code != 200:
            print(f"Erreur API: {r.status_code}")
            print(r.text[:500])
            break
        if "data" in response:
            count = 0
            for paper in response["data"]:
                if paper["paperId"] in seen_papers:
                    print(f"Paper {paper['paperId']} already seen")
                    continue
                else : 
                    seen_papers.add(paper["paperId"])
                    print(json.dumps(paper), file=file)
                    count += 1
            retrieved += count
            print(f"Retrieved {retrieved} papers...")
        # checks for continuation token to get next batch of results
        if "token" not in response:
            break
        if response["token"] in seen_tokens:
            break
        seen_tokens.add(response["token"])
        next_params = {**query_params, "token": response["token"]}
        r = requests.get(url, params=next_params, headers=headers)
        response = r.json()

print(f"Done! Retrieved {retrieved} papers total")

#Conversion de NDJSON vers JSON :


input_path = "transformers_papers.json"          # NDJSON
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