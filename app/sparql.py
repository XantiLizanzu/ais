from datetime import datetime
from io import BytesIO
from pathlib import Path
from fastapi import FastAPI, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from rdflib import RDF, Graph, Literal, Namespace, URIRef
import networkx as nx
import matplotlib.pyplot as plt
from rdflib.extras.external_graph_libs import rdflib_to_networkx_multidigraph

OTL = Namespace("https://data.rws.nl/def/otl/")
NEN2767 = Namespace("https://data.rws.nl/def/nen2767/")

GRAPH_FILE = Path("/data/knowledge_graph.ttl")

def load_graph():
    g = Graph()
    if GRAPH_FILE.exists():
        g.parse(GRAPH_FILE, format="turtle")
    else:
        # Initialize graph with default data
        g = Graph()
        oosterscheldekering = URIRef("https://data.rws.nl/data/oosterscheldekering")
        g.add((oosterscheldekering, RDF.type, OTL.StormSearchBarrier))
        
        oosterscheldekering_part1 = URIRef("https://data.rws.nl/data/oosterscheldekering_part0")
        g.add((oosterscheldekering_part1, RDF.type, OTL.Part))
        g.add((oosterscheldekering, OTL.hasPart, oosterscheldekering_part1))
        part1_inspection = URIRef(f"https://data.rws.nl/data/inspection_0")
        g.add((oosterscheldekering_part1, OTL.hasInspection, part1_inspection))
        g.add((part1_inspection, RDF.type, OTL.Inspection))
        part1_inspection_score = URIRef(f"https://data.rws.nl/data/inspection_score_0")
        g.add((part1_inspection, OTL.hasNEN2767Condition, part1_inspection_score))
        g.add((part1_inspection_score, RDF.value, NEN2767.Good))
        g.add((part1_inspection_score, RDF.type, NEN2767.ConditionScore))
        g.add((part1_inspection, OTL.inspectionDate, Literal(datetime(2025, 1, 1).date())))
        
        part2_inspection = URIRef(f"https://data.rws.nl/data/inspection_1")
        g.add((oosterscheldekering_part1, OTL.hasInspection, part2_inspection))
        g.add((part2_inspection, RDF.type, OTL.Inspection))
        part2_inspection_score = URIRef(f"https://data.rws.nl/data/inspection_score_1")
        g.add((part2_inspection, OTL.hasNEN2767Condition, part2_inspection_score))
        g.add((part2_inspection_score, RDF.value, NEN2767.BelowAverage))
        g.add((part2_inspection_score, RDF.type, NEN2767.ConditionScore))
        g.add((part2_inspection, OTL.inspectionDate, Literal(datetime(2026, 1, 1).date())))
    return g

app = FastAPI(title="Beheerobjecten Statuses API", 
              summary="Retrieval API for Beheerobjecten Statuses. Retrieves information through SPARQL queries from the knowledge graph.", 
              version="0.0.0")

# @app.get("/knowledge-graph/")
# async def get_knowledge_graph() -> StreamingResponse:
#     g = load_graph()
#     G = rdflib_to_networkx_multidigraph(g)

#     plt.figure(figsize=(12, 12))

#     pos = nx.spectral_layout(G, scale=0.5, center=(0, 0))

#     labels = {n: "/".join(n.split("/")[::-1][:2][::-1]) if isinstance(n, str) else str(n) for n in G.nodes()}

#     nx.draw(
#         G,
#         pos,
#         labels=labels,
#         with_labels=True,
#         node_size=1000,
#         node_color="lightblue",
#         edge_color="lightgray",
#     )

#     edge_labels = {(u, v): k.split("/")[-1].split("#")[-1] if isinstance(k, str) else str(k) for u, v, k in G.edges(keys=True)}
#     nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="blue")

#     plt.margins(0.2)
#     # plt.tight_layout()

#     buf = BytesIO()
#     plt.savefig(buf, format="png")
#     buf.seek(0)
#     plt.close()
    
#     return StreamingResponse(buf, media_type="image/png")

@app.get("/status/{asset_id}/{part_id}")
def get_status(asset_id: str, part_id: int) -> list[tuple[str, str]]:
    g = load_graph()
    sparql_query = f"""
    PREFIX otl: <https://data.rws.nl/def/otl/>
    PREFIX ex: <https://data.rws.nl/data/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?condition_value ?inspection_date WHERE {{
        ex:{asset_id} otl:hasPart ?part .
        FILTER(STRENDS(STR(?part), "/data/{asset_id}_part{part_id}"))
        ?part otl:hasInspection ?inspection .
        ?inspection otl:hasNEN2767Condition ?condition .
        ?condition rdf:value ?condition_value .
        ?inspection otl:inspectionDate ?inspection_date .
    }}
    """
    qres = g.query(sparql_query)
    if not qres:
        return None
    
    return [(str(row[0]), str(row[1])) for row in qres]