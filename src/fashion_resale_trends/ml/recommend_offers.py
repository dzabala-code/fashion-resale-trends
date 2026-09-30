from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

from fashion_resale_trends.keywords import title_contains_keyword
from fashion_resale_trends.storage import ObjectStore


OUTPUT_COLUMNS = [
    "keyword",
    "keyword_rank",
    "trend_score",
    "offer_rank",
    "item_id",
    "title",
    "price_value",
    "currency",
    "condition",
    "item_url",
    "seller_feedback_score",
    "seller_feedback_percentage",
    "model_score",
    "source_is_fixture",
]


def condition_score(value: object) -> float:
    condition = str(value or "").lower()
    if "neuf" in condition or "new" in condition:
        return 1.0
    if "tres bon" in condition or "very good" in condition:
        return 0.85
    if "occasion" in condition or "used" in condition:
        return 0.65
    return 0.5


def add_offer_features(data: pd.DataFrame) -> pd.DataFrame:
    result = data.copy()
    result["price_value"] = pd.to_numeric(result["price_value"], errors="coerce").fillna(0.0)
    result["seller_feedback_score"] = pd.to_numeric(result.get("seller_feedback_score", 0), errors="coerce").fillna(0.0)
    result["seller_feedback_percentage"] = pd.to_numeric(result.get("seller_feedback_percentage", 0), errors="coerce").fillna(0.0)
    result["shipping_cost"] = pd.to_numeric(result.get("shipping_cost", 0), errors="coerce").fillna(0.0)
    if "condition" not in result.columns:
        result["condition"] = ""
    result["condition_score"] = result["condition"].map(condition_score)
    result["title_keyword_match"] = [
        1.0 if title_contains_keyword(str(title), str(keyword)) else 0.0
        for title, keyword in zip(result["title"], result["keyword"])
    ]
    if "source_is_fixture" not in result.columns:
        result["source_is_fixture"] = False
    result["source_quality_flags"] = result["source_is_fixture"].map(lambda value: 0.5 if bool(value) else 1.0)
    max_price = max(float(result["price_value"].max()), 1.0)
    result["price_attractiveness"] = 1.0 - (result["price_value"] / max_price).clip(0, 1)
    result["opportunity_label"] = (
        result["trend_score"].astype(float) * 0.40
        + result["price_attractiveness"] * 0.20
        + result["condition_score"] * 0.15
        + (result["seller_feedback_percentage"] / 100.0).clip(0, 1) * 0.10
        + result["title_keyword_match"] * 0.10
        + result["source_quality_flags"] * 0.05
    )
    return result


def recommend(top_keywords: pd.DataFrame, offers: pd.DataFrame) -> pd.DataFrame:
    merged = offers.merge(top_keywords[["keyword", "keyword_rank", "trend_score"]], on="keyword", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    featured = add_offer_features(merged)
    feature_columns = [
        "trend_score",
        "price_value",
        "seller_feedback_score",
        "seller_feedback_percentage",
        "condition_score",
        "shipping_cost",
        "title_keyword_match",
        "source_quality_flags",
    ]
    
    X = featured[feature_columns]
    y = featured["opportunity_label"]
    if len(X) > 10:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        model = RandomForestRegressor(n_estimators=120, random_state=42, min_samples_leaf=3)
        model.fit(X_train, y_train)
        test_mae = float(np.mean(np.abs(model.predict(X_test) - y_test.to_numpy())))
        print(f"RandomForest — MAE on 20 % hold-out: {test_mae:.4f}")
    else:
       
        model = RandomForestRegressor(n_estimators=120, random_state=42, min_samples_leaf=3)
        model.fit(X, y)
    featured["model_score"] = model.predict(featured[feature_columns])
    featured = featured.sort_values(["keyword_rank", "model_score", "price_value"], ascending=[True, False, True])
    featured["offer_rank"] = featured.groupby("keyword").cumcount() + 1
    selected = featured[featured["offer_rank"] <= 3].copy()
    selected["currency"] = selected.get("currency", "EUR")
    return selected[OUTPUT_COLUMNS].sort_values(["keyword_rank", "offer_rank"]).reset_index(drop=True)


def _single_csv(path: Path) -> Path:
    matches = sorted((path / "csv").glob("*.csv"))
    if not matches:
        raise FileNotFoundError(f"No CSV part found in {path / 'csv'}")
    return matches[0]


def main() -> None:
    store = ObjectStore()
    top_path = store.layer_dir("combined", "keyword_trend_scores", "top10")
    offers_path = store.layer_dir("formatted", "marketplace", "ebay")
    top_keywords = pd.read_parquet(top_path / "parquet")
    offers = pd.read_parquet(offers_path / "parquet")
    recommendations = recommend(top_keywords, offers)

    output = store.layer_dir("ml", "ebay_offer_recommendations", "top3")
    output.mkdir(parents=True, exist_ok=True)
    recommendations.to_parquet(output / "recommendations.parquet", index=False)
    recommendations.to_csv(output / "top3_ebay_offers.csv", index=False)
    store.upload_tree_if_enabled(output, "ml/ebay_offer_recommendations/top3")
    print(f"Wrote {len(recommendations)} recommendations to {output}")


if __name__ == "__main__":
    main()
