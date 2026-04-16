import json
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ⚠️ Note : Il est déconseillé de laisser des clés API en clair dans le code. 
# Pense à utiliser os.environ.get("S2_API_KEY") en production.
API_KEY = "2y5uBuUCdd49YFT5Jsuhv5LGG2Wfh5CJaLIZfFz5"
GRAPH_BASE = "https://api.semanticscholar.org/graph/v1"

# Demande exhaustive de tous les champs pour avoir le MAXIMUM d'informations
MAX_FIELDS = (
    "paperId,url,title,abstract,venue,publicationVenue,year,referenceCount,"
    "citationCount,influentialCitationCount,isOpenAccess,openAccessPdf,"
    "fieldsOfStudy,s2FieldsOfStudy,publicationTypes,publicationDate,journal,"
    "citationStyles,authors,tldr,"
    "citations.paperId,citations.title,citations.year,"
    "references.paperId,references.title,references.year"
)

def get_session():
    """Configure une session HTTP avec gestion automatique des retries (erreurs & rate limits)."""
    session = requests.Session()
    session.headers.update({"x-api-key": API_KEY})
    retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def main():
    session = get_session()
    
    print("1. Recherche des IDs des papiers via Bulk Search...")
    paper_ids = []
    token = None
    
    # Étape 1 : Récupérer uniquement les IDs (très rapide)
    while True:
        params = {"query": "Retrieval Augmented Generation", "fields": "paperId", "limit": 1000}
        if token: params["token"] = token
        
        response = session.get(f"{GRAPH_BASE}/paper/search/bulk", params=params).json()
        paper_ids.extend([p["paperId"] for p in response.get("data", []) if p.get("paperId")])
        
        token = response.get("token")
        if not token: break
        time.sleep(1.1) # Respect du rate limit

    print(f"-> {len(paper_ids)} IDs trouvés.\n2. Récupération des détails maximums via Batch POST...")
    
    # Étape 2 : Récupérer le maximum d'infos via le endpoint POST /paper/batch
    with open("machine_learning_papers_optimal.jsonl", "w", encoding="utf-8") as f:
        # Découpage natif en lots de 500 comme recommandé par la doc
        for i in range(0, len(paper_ids), 500):
            chunk = paper_ids[i:i+500]
            batch_response = session.post(
                f"{GRAPH_BASE}/paper/batch",
                params={"fields": MAX_FIELDS},
                json={"ids": chunk}
            ).json()
            
            # Sauvegarde immédiate
            for paper in batch_response:
                if paper: # Ignorer les potentiels retours nuls
                    f.write(json.dumps(paper, ensure_ascii=False) + "\n")
            
            print(f"Progression : {min(i+500, len(paper_ids))} / {len(paper_ids)}")
            time.sleep(1.1)

    print("Terminé ! Données sauvegardées avec succès.")

if __name__ == "__main__":
    main()