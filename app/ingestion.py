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

# Define graph file path
GRAPH_FILE = Path("/data/knowledge_graph.ttl")
GRAPH_FILE.parent.mkdir(parents=True, exist_ok=True)

# Load or initialize graph
g = Graph()
if GRAPH_FILE.exists():
    g.parse(GRAPH_FILE, format="turtle")
else:
    # Initialize graph with default data
    oosterscheldekering = URIRef("https://data.rws.nl/data/oosterscheldekering")
    g.add((oosterscheldekering, RDF.type, OTL.StormSearchBarrier))
    oosterscheldekering_part1 = URIRef("https://data.rws.nl/data/oosterscheldekering_part0")
    g.add((oosterscheldekering_part1, RDF.type, OTL.Part))
    g.add((oosterscheldekering, OTL.hasPart, oosterscheldekering_part1))
    g.serialize(destination=GRAPH_FILE, format="turtle")

inspection_n = 0


class DiskInspection(BaseModel):
    asset_id: int
    component_id: int
    condition: str
    inspection_date: datetime


app = FastAPI(title="Ingestion Service", 
              version="0.0.0", 
              summary="Ingestion service for the Rijkswaterstaat Beheerobjecten knowledge graph. Able to ingest data from external systems (DISK, Ultimo, and Meridian).")


@app.get("/knowledge-graph/")
async def get_knowledge_graph() -> StreamingResponse:
    G = rdflib_to_networkx_multidigraph(g)

    plt.figure(figsize=(12, 12))

    pos = nx.spectral_layout(G, scale=0.5, center=(0, 0))

    labels = {n: "/".join(n.split("/")[::-1][:2][::-1]) if isinstance(n, str) else str(n) for n in G.nodes()}

    nx.draw(
        G,
        pos,
        labels=labels,
        with_labels=True,
        node_size=1000,
        node_color="lightblue",
        edge_color="lightgray",
    )

    edge_labels = {(u, v): k.split("/")[-1].split("#")[-1] if isinstance(k, str) else str(k) for u, v, k in G.edges(keys=True)}
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="blue")

    plt.margins(0.2)
    # plt.tight_layout()

    buf = BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()
    
    return StreamingResponse(buf, media_type="image/png")

@app.post("/disk-inspections/")
def ingest_disk_inspections(disk_inspections: list[DiskInspection]) -> None:
    global inspection_n
    oosterscheldekering_part1 = URIRef("https://data.rws.nl/data/oosterscheldekering_part0")
    part1_inspection = URIRef(f"https://data.rws.nl/data/inspection_{inspection_n}")
    g.add((oosterscheldekering_part1, OTL.hasInspection, part1_inspection))
    g.add((part1_inspection, RDF.type, OTL.Inspection))
    part1_inspection_score = URIRef(f"https://data.rws.nl/data/inspection_score_{inspection_n}")
    inspection_n += 1
    g.add((part1_inspection, OTL.hasNEN2767Condition, part1_inspection_score))
    g.add((part1_inspection_score, RDF.value, NEN2767.Good))
    g.add((part1_inspection_score, RDF.type, NEN2767.ConditionScore))
    g.serialize(destination=GRAPH_FILE, format="turtle")
    # part 1 has inspection date of 1 Jan 2025
    g.add((part1_inspection, OTL.inspectionDate, Literal(datetime(2025, 1, 1).date())))

@app.post("/reports/")
def ingest_reports(files: list[UploadFile]):
    global inspection_n
    part2_inspection = URIRef(f"https://data.rws.nl/data/inspection_{inspection_n}")
    g.add((oosterscheldekering_part1, OTL.hasInspection, part2_inspection))
    g.add((part2_inspection, RDF.type, OTL.Inspection))
    part2_inspection_score = URIRef(f"https://data.rws.nl/data/inspection_score_{inspection_n}")
    inspection_n += 1
    g.add((part2_inspection, OTL.hasNEN2767Condition, part2_inspection_score))
    g.add((part2_inspection_score, RDF.value, NEN2767.BelowAverage))
    g.add((part2_inspection_score, RDF.type, NEN2767.ConditionScore))
    g.add((part2_inspection, OTL.inspectionDate, Literal(datetime(2026, 1, 1).date())))
    g.serialize(destination=GRAPH_FILE, format="turtle")
