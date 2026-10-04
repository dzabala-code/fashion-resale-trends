
from __future__ import annotations

import json
import os

import requests

KIBANA = os.getenv("KIBANA_URL", "http://localhost:5602")
ELASTICSEARCH = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201")
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

KW_DV = "bbcbc4e3-e01a-4680-9a48-37accb81c3bc"
OFFER_DV = "e0caa52f-b3b7-4141-ab1f-634bb3fe0912"
TS_DV = "c1a2b3d4-e5f6-7890-abcd-ef1234567890"


def verify_elasticsearch_indices() -> bool:
    """Ensure indices exist and contain documents before building Lens panels."""
    ok = True
    for index in ("fashion_keyword_scores", "fashion_offer_recommendations"):
        try:
            r = requests.get(f"{ELASTICSEARCH}/{index}/_count", timeout=10)
            if r.status_code == 404:
                print(f"  MISSING index: {index} — run reindex first (see scripts/reindex_and_dashboard.sh)")
                ok = False
                continue
            count = r.json().get("count", 0)
            if count == 0:
                print(f"  EMPTY index: {index} — run reindex first")
                ok = False
            else:
                print(f"  {index}: {count} documents")
        except requests.RequestException as exc:
            print(f"  Cannot reach Elasticsearch at {ELASTICSEARCH}: {exc}")
            ok = False
    return ok


def _terms_field(field: str) -> str:
    """Use .keyword subfield for text fields; bare name if already keyword."""
    return field if field.endswith(".keyword") else f"{field}.keyword"


def create_data_views() -> None:
    """Create or update Kibana data views (idempotent)."""
    views = [
        {"id": KW_DV, "title": "fashion_keyword_scores", "timeFieldName": "collected_at_utc"},
        {"id": OFFER_DV, "title": "fashion_offer_recommendations", "timeFieldName": ""},
        {"id": TS_DV, "title": "fashion_trends_timeseries", "timeFieldName": "date"},
    ]
    for v in views:
        body = {"data_view": {"id": v["id"], "title": v["title"], "timeFieldName": v["timeFieldName"]}}
        r = requests.post(
            f"{KIBANA}/api/data_views/data_view",
            headers=HEADERS, json=body, timeout=15,
        )
        d = r.json()
        if "data_view" in d:
            print(f"  Data view created: {v['title']}")
            continue

        message = str(d.get("message", d))
        is_duplicate = r.status_code in (400, 409) and "Duplicate" in message
        if is_duplicate or r.status_code == 409:
            
            requests.put(
                f"{KIBANA}/api/data_views/data_view/{v['id']}",
                headers=HEADERS, json=body, timeout=15,
            )
            print(f"  Data view already exists (ok): {v['title']}")
            continue

        print(f"  Data view {v['title']}: {message[:120]}")


def create_lens_timeseries(obj_id: str, title: str, dv_id: str) -> str:
    """Ligne temporelle : interest moyen par semaine, top 10 keywords."""
    body = {
        "attributes": {
            "title": title,
            "visualizationType": "lnsXY",
            "state": {
                "datasourceStates": {
                    "formBased": {
                        "layers": {
                            "layer1": {
                                "columnOrder": ["col_date", "col_kw", "col_interest"],
                                "columns": {
                                    "col_date": {
                                        "label": "Date",
                                        "dataType": "date",
                                        "operationType": "date_histogram",
                                        "scale": "interval",
                                        "sourceField": "date",
                                        "isBucketed": True,
                                        "customLabel": False,
                                        "params": {
                                            "interval": "1w",
                                            "includeEmptyRows": False,
                                            "dropPartials": False,
                                        },
                                    },
                                    "col_kw": {
                                        "label": "Keyword",
                                        "dataType": "string",
                                        "operationType": "terms",
                                        "scale": "ordinal",
                                        "sourceField": "keyword",
                                        "isBucketed": True,
                                        "customLabel": False,
                                        "params": {
                                            "size": 10,
                                            "orderBy": {"type": "column", "columnId": "col_interest"},
                                            "orderDirection": "desc",
                                            "otherBucket": True,
                                            "missingBucket": False,
                                            "parentFormat": {"id": "terms"},
                                            "include": [],
                                            "exclude": [],
                                            "includeIsRegex": False,
                                            "excludeIsRegex": False,
                                        },
                                    },
                                    "col_interest": {
                                        "label": "Intérêt moyen",
                                        "dataType": "number",
                                        "operationType": "avg",
                                        "scale": "ratio",
                                        "sourceField": "interest",
                                        "isBucketed": False,
                                        "customLabel": False,
                                        "params": {},
                                    },
                                },
                                "indexPatternId": dv_id,
                            }
                        }
                    }
                },
                "visualization": {
                    "legend": {"isVisible": True, "position": "right"},
                    "valueLabels": "hide",
                    "fittingFunction": "None",
                    "layers": [{
                        "layerId": "layer1",
                        "layerType": "data",
                        "accessors": ["col_interest"],
                        "xAccessor": "col_date",
                        "splitAccessor": "col_kw",
                        "seriesType": "line",
                        "yConfig": [],
                    }],
                },
                "query": {"language": "kuery", "query": ""},
                "filters": [],
            },
        },
        "references": [{"id": dv_id, "name": "indexpattern-datasource-layer-layer1", "type": "index-pattern"}],
    }
    r = requests.post(f"{KIBANA}/api/saved_objects/lens/{obj_id}?overwrite=true", headers=HEADERS, json=body, timeout=15)
    d = r.json()
    return d.get("id", f"ERR: {str(d)[:120]}")


def create_lens_bar(
    obj_id: str,
    title: str,
    dv_id: str,
    x_field: str,
    y_field: str,
    series_type: str = "bar_horizontal",
    operation: str = "max",
) -> str:
    body = {
        "attributes": {
            "title": title,
            "visualizationType": "lnsXY",
            "state": {
                "datasourceStates": {
                    "formBased": {
                        "layers": {
                            "layer1": {
                                "columnOrder": ["col_x", "col_y"],
                                "columns": {
                                    "col_x": {
                                        "label": x_field,
                                        "dataType": "string",
                                        "operationType": "terms",
                                        "scale": "ordinal",
                                        "sourceField": _terms_field(x_field),
                                        "isBucketed": True,
                                        "customLabel": False,
                                        "params": {
                                            "size": 10,
                                            "orderBy": {"type": "column", "columnId": "col_y"},
                                            "orderDirection": "desc",
                                            "otherBucket": True,
                                            "missingBucket": False,
                                            "parentFormat": {"id": "terms"},
                                            "include": [],
                                            "exclude": [],
                                            "includeIsRegex": False,
                                            "excludeIsRegex": False,
                                        },
                                    },
                                    "col_y": {
                                        "label": y_field,
                                        "dataType": "number",
                                        "operationType": operation,
                                        "scale": "ratio",
                                        "sourceField": y_field,
                                        "isBucketed": False,
                                        "customLabel": False,
                                        "params": {},
                                    },
                                },
                                "indexPatternId": dv_id,
                            }
                        }
                    }
                },
                "visualization": {
                    "legend": {"isVisible": True, "position": "right"},
                    "valueLabels": "show",
                    "fittingFunction": "None",
                    "layers": [{
                        "layerId": "layer1",
                        "layerType": "data",
                        "accessors": ["col_y"],
                        "xAccessor": "col_x",
                        "seriesType": series_type,
                        "yConfig": [],
                    }],
                },
                "query": {"language": "kuery", "query": ""},
                "filters": [],
            },
        },
        "references": [{"id": dv_id, "name": "indexpattern-datasource-layer-layer1", "type": "index-pattern"}],
    }
    r = requests.post(f"{KIBANA}/api/saved_objects/lens/{obj_id}?overwrite=true", headers=HEADERS, json=body, timeout=15)
    d = r.json()
    return d.get("id", f"ERR: {str(d)[:120]}")


def create_lens_pie(obj_id: str, title: str, dv_id: str, slice_field: str, value_field: str) -> str:
    body = {
        "attributes": {
            "title": title,
            "visualizationType": "lnsPie",
            "state": {
                "datasourceStates": {
                    "formBased": {
                        "layers": {
                            "layer1": {
                                "columnOrder": ["col_slice", "col_value"],
                                "columns": {
                                    "col_slice": {
                                        "label": slice_field,
                                        "dataType": "string",
                                        "operationType": "terms",
                                        "scale": "ordinal",
                                        "sourceField": _terms_field(slice_field),
                                        "isBucketed": True,
                                        "customLabel": False,
                                        "params": {
                                            "size": 10,
                                            "orderBy": {"type": "column", "columnId": "col_value"},
                                            "orderDirection": "desc",
                                            "otherBucket": True,
                                            "missingBucket": False,
                                            "parentFormat": {"id": "terms"},
                                            "include": [],
                                            "exclude": [],
                                            "includeIsRegex": False,
                                            "excludeIsRegex": False,
                                        },
                                    },
                                    "col_value": {
                                        "label": value_field,
                                        "dataType": "number",
                                        "operationType": "avg",
                                        "scale": "ratio",
                                        "sourceField": value_field,
                                        "isBucketed": False,
                                        "customLabel": False,
                                        "params": {},
                                    },
                                },
                                "indexPatternId": dv_id,
                            }
                        }
                    }
                },
                "visualization": {
                    "shape": "donut",
                    "layers": [{
                        "layerId": "layer1",
                        "layerType": "data",
                        "primaryGroups": ["col_slice"],
                        "metrics": ["col_value"],
                        "numberDisplay": "percent",
                        "categoryDisplay": "default",
                        "legendDisplay": "default",
                    }],
                },
                "query": {"language": "kuery", "query": ""},
                "filters": [],
            },
        },
        "references": [{"id": dv_id, "name": "indexpattern-datasource-layer-layer1", "type": "index-pattern"}],
    }
    r = requests.post(f"{KIBANA}/api/saved_objects/lens/{obj_id}?overwrite=true", headers=HEADERS, json=body, timeout=15)
    d = r.json()
    return d.get("id", f"ERR: {str(d)[:120]}")


def create_dashboard(panels: list, references: list) -> str:
    body = {
        "attributes": {
            "title": "Fashion Resale Trends — Tableau de bord",
            "description": "Top tendances mode France/Europe et meilleures offres eBay",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "hidePanelTitles": False}),
            "version": 1,
            # Keyword scores are timestamped: without a saved range, Kibana's default
            # "last 15 minutes" hides every run indexed earlier.
            "timeRestore": True,
            "timeFrom": "now-30d",
            "timeTo": "now",
            "refreshInterval": {"pause": True, "value": 0},
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": references,
    }
    r = requests.post(
        f"{KIBANA}/api/saved_objects/dashboard/fashion-resale-dashboard?overwrite=true",
        headers=HEADERS, json=body, timeout=15
    )
    d = r.json()
    return d.get("id", f"ERR: {str(d)[:200]}")


def main() -> None:
    print("Checking Elasticsearch indices...")
    if not verify_elasticsearch_indices():
        print("\nAborting dashboard creation — index data first:")
        print("  ELASTICSEARCH_URL=http://localhost:9201 PYTHONPATH=src python -m fashion_resale_trends.exposition.index_elasticsearch")
        print("  or: ./scripts/reindex_and_dashboard.sh")
        raise SystemExit(1)

    print("\nCreating data views...")
    create_data_views()
    print("\nCréation des visualisations...")

    id1 = create_lens_bar("lens-trend-scores", "Top Keywords — Trend Score", KW_DV, "keyword", "trend_score")
    print(f"  1. Trend Score:        {id1}")

    id2 = create_lens_bar("lens-reddit-mentions", "Posts Reddit par keyword", KW_DV, "keyword", "reddit_mentions")
    print(f"  2. Reddit:             {id2}")

    id3 = create_lens_bar("lens-media-score", "Score mentions presse", KW_DV, "keyword", "media_score")
    print(f"  3. Media Score:        {id3}")

    id4 = create_lens_bar("lens-ebay-price", "Prix moyen eBay par keyword", OFFER_DV, "keyword", "price_value", operation="average")
    print(f"  4. Prix eBay:          {id4}")

    id5 = create_lens_bar("lens-conditions", "Offers by condition", OFFER_DV, "condition", "price_value")
    print(f"  5. Conditions:         {id5}")

    id6 = create_lens_bar("lens-google-momentum", "Croissance Google Trends (3 mois vs année précédente)", KW_DV, "keyword", "google_momentum")
    print(f"  6. Momentum Google:    {id6}")

    print("\nAssemblage du dashboard...")
    # 6 graphiques en grille 2×3
    panels = [
        {"version": "8.13.4", "type": "lens", "gridData": {"x": 0,  "y": 0,  "w": 24, "h": 15, "i": "p1"}, "panelIndex": "p1", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p1"},
        {"version": "8.13.4", "type": "lens", "gridData": {"x": 24, "y": 0,  "w": 24, "h": 15, "i": "p2"}, "panelIndex": "p2", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p2"},
        {"version": "8.13.4", "type": "lens", "gridData": {"x": 0,  "y": 15, "w": 24, "h": 15, "i": "p3"}, "panelIndex": "p3", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p3"},
        {"version": "8.13.4", "type": "lens", "gridData": {"x": 24, "y": 15, "w": 24, "h": 15, "i": "p4"}, "panelIndex": "p4", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p4"},
        {"version": "8.13.4", "type": "lens", "gridData": {"x": 0,  "y": 30, "w": 24, "h": 15, "i": "p5"}, "panelIndex": "p5", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p5"},
        {"version": "8.13.4", "type": "lens", "gridData": {"x": 24, "y": 30, "w": 24, "h": 15, "i": "p6"}, "panelIndex": "p6", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p6"},
    ]
    all_ids = [id1, id2, id3, id4, id5, id6]
    refs = [{"id": vid, "name": f"panel_p{i+1}", "type": "lens"} for i, vid in enumerate(all_ids)]

    dash_id = create_dashboard(panels, refs)
    if dash_id.startswith("ERR"):
        print(f"\n Erreur dashboard: {dash_id}")
    else:
        print(f"\n Dashboard créé !")
        print(f" http://localhost:5602/app/dashboards#/view/{dash_id}")


if __name__ == "__main__":
    main()
