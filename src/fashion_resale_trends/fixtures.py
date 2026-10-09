from __future__ import annotations

from datetime import datetime, timedelta, timezone


FIXTURE_AT = "2026-06-03T00:00:00+00:00"

KEYWORDS = [
    "trench coat",
    "mocassins",
    "ballet flats",
    "sac baguette",
    "jean brut",
    "veste en cuir",
    "jupe satin",
    "cardigan",
    "samba adidas",
    "leopard print",
]

MEDIA_FIXTURES = {
    "vogue": [
        "gorpcore s'invite dans les défilés printemps 2026",
        "quiet luxury la tendance qui redéfinit l'élégance",
        "robe satin maxi la pièce star de la saison",
        "old money aesthetic le retour du chic discret",
        "ballet flats le grand retour des ballerines plates",
        "oversized blazer la veste XXL qui s'impose",
        "samba adidas sneaker tendance des it-girls",
        "crochet top l'incontournable de l'été",
        "wide leg pantalon jambe large la coupe du moment",
        "mob wife aesthetic la tendance glamour qui cartonne",
        "leopard print imprimé léopard grande tendance automne",
        "slip dress la robe nuisette revisitée",
        "kitten heels talons chaton tendance parisienne",
        "denim on denim le total look jean revient",
        "maxi jupe longue tendance bohème chic",
    ],
    "vogue_tendances": [
        "capsule wardrobe les pièces essentielles 2026",
        "coquette aesthetic la tendance ultra-féminine",
        "cargo pants pantalon cargo mode urbaine",
        "barbiecore le rose règne encore cette saison",
        "trench coat la pièce signature du printemps",
        "Y2K revival le retour des années 2000 en force",
        "dark academia tendance intellectuelle et romantique",
        "pull cachemire luxe accessible les meilleures adresses",
        "veste en cuir perfecto tendance intemporelle",
        "platform shoes semelles compensées nouvelle génération",
        "jupe plissée la coupe légère de l'été",
        "boho chic le style bohème fait son grand retour",
        "mary jane chaussure tendance du moment",
        "mesh flats sandales résille incontournables",
        "manteau camel la couleur phare de l'automne",
    ],
    "vogue_shopping": [
        "sac baguette iconique de retour chez toutes les marques",
        "new balance 530 sneaker tendance sélection",
        "veja sneakers éco-responsables les plus portées",
        "birkenstock sandales tendance sélection été 2026",
        "repetto ballerines sélection chaussures iconiques",
        "sézane collection printemps les pièces à shopper",
        "jacquemus minimalisme méditerranéen collection été",
        "arket basics durables les essentiels de la saison",
        "toteme silhouette épurée collection automne",
        "cos design scandinave sélection shopping",
        "isabel marant esprit bohème parisien collection",
        "maje robe portefeuille tendance femme",
        "sandro blazer structuré sélection automne",
        "uniqlo essentiels qualité prix rapport idéal",
        "zara tendances mode sélection collection saison",
    ],
    "stylist": [
        "gorpcore le style outdoor envahit la ville",
        "quiet luxury minimalisme luxueux la tendance durable",
        "jean brut raw denim le retour du jean brut",
        "cardigan tricoté main tendance workwear chic",
        "chemise oversize chemise grande taille look décontracté",
        "mocassins chaussures plates tendance automne",
        "tailoring tailleur structuré tendance bureau",
        "jupe satin matières luxueuses la jupe satin revient",
        "denim jacket veste en jean intemporelle",
        "ballet flats ballerines chic sélection shopping",
        "streetwear luxe fusion sport et élégance",
        "coquette aesthetic dentelle ruban ultra-féminin",
        "techwear fonctionnel et stylé la mode tech s'installe",
        "slip dress robe lingerie tendance printemps",
        "pantalon large wide leg la coupe confort chic",
    ],
    "elle_tendances": [
        "barbiecore le rose fushia envahit les garde-robes",
        "Y2K aesthetic tendance nostalgie années 2000",
        "dark academia vêtements inspirés des universités anglaises",
        "coastal grandmother style balnéaire chic mature",
        "mob wife aesthetic fourrure glamour tendance hiver",
        "clean girl aesthetic minimalisme naturel sans effort",
        "cottage core robe florale campagne romantique",
        "athleisure sport chic le style actif au quotidien",
        "preppy style college américain tendance europe",
        "dopamine dressing s'habiller coloré pour se sentir bien",
        "normcore le anti-fashion qui revient en force",
        "grunge revival le retour du style rock années 90",
        "workwear tailleur professionnel tendance 2026",
        "old money aesthetic luxe sobre et intemporel",
        "boho style bohème festival et quotidien",
    ],
    "madame_figaro": [
        "quiet luxury les marques qui incarnent le luxe discret",
        "trench coat burberry vs sézane le match tendance",
        "jupe plissée la coupe légère incontournable printemps",
        "pull mohair douceur matière tendance hiver",
        "bottines chelsea boots tendance automne hiver",
        "robe portefeuille wrap dress flatteuse polyvalente",
        "manteau camel couleur intemporelle investissement mode",
        "tailleur pantalon femme look professionnel chic",
        "ceinture accessoire structurant tendance silhouette",
        "écharpe soie carré hermès tendance accessoires",
        "lunettes oversize accessoire statement tendance",
        "cardigan cachemire sézane uniqlo comparatif",
        "sac tote bag canvas tendance pratique",
        "mocassins gucci vs zara tendance dupe mocassins",
        "veste teddy teddy jacket tendance cocooning chic",
    ],
    "grazia": [
        "samba adidas la sneaker que tout le monde porte",
        "ballet core tendance danse classique dans la mode",
        "crochet top le haut crochet bohème tendance été",
        "oversized blazer veste boyfriend oversize tendance",
        "maxi skirt jupe longue tendance bohème",
        "platform mules mules compensées tendance été 2026",
        "leopard print retour en force de l'imprimé léopard",
        "denim on denim total look jean les meilleures façons",
        "mary jane soulier tendance chaussures plates chic",
        "kitten heels petit talon élégant tendance parisienne",
        "mesh top top résille transparence tendance été",
        "barbiecore les pièces roses à shopper maintenant",
        "gorpcore vêtements outdoor tendance urbaine",
        "slip dress robe satin nuisette tendance soirée",
        "sac baguette it bag parisien tendance",
    ],
    "marie_claire": [
        "capsule wardrobe minimaliste construire sa garde-robe",
        "wide leg jean jambe large coupe tendance 2026",
        "robe satin robe satin longue soirée et quotidien",
        "chemise oversize comment porter la chemise XXL",
        "jupe midi longueur mi-mollet tendance bureau",
        "pull cachemire investir dans un pull cachemire",
        "veste en cuir perfecto iconique tendance rock",
        "sneakers new balance running esthétique tendance",
        "tote bag canvas grand fourre-tout tendance",
        "boucles oreilles statement bijoux tendance",
        "pantalon droit coupe droite classique tendance",
        "robe florale fleuri tendance printemps été",
        "jean boyfriend jean large relaxed fit tendance",
        "manteau laine investissement mode durable",
        "sandales plates confortables tendance été",
    ],
    "glamour": [
        "Y2K revival les pièces des années 2000 que l'on rachète",
        "coquette aesthetic romantique ruban dentelle tendance",
        "gorpcore outdoorsy look en ville tendance",
        "mob wife glamour excès fourrure tendance hiver",
        "ballet flats ballerines tendance street style paris",
        "denim jacket veste jean oversize tendance",
        "maxi robe longue tendance détente et soirée",
        "platform boots bottines semelles épaisses tendance",
        "barbiecore style barbie pièces roses selection",
        "pull oversize grand pull tendance cocooning chic",
        "jupe courte mini jupe tendance printemps",
        "body top body tendance slim silhouette",
        "ceinture large accessoire bohème tendance boho",
        "athleisure legging ensemble sport tendance rue",
        "cargo pants pantalon cargo tendance urbaine",
    ],
    "lofficiel": [
        "old money aesthetic tendance discrétion luxe",
        "dark academia look inspiré cambridge oxford tendance",
        "jacquemus ss26 collection méditerranéenne analyse",
        "toteme minimalisme scandinave tendance intemporelle",
        "acne studios coupe structurée tendance saison",
        "isabel marant esprit rock parisien tendance",
        "sézane romantisme parisien collection automne",
        "quiet luxury the row celine tendance luxe calme",
        "blazer croisé double boutonnage tendance tailoring",
        "robe midi longueur genou tendance polyvalente",
        "cuir matière tendance toutes saisons perfecto veste",
        "velours matière tendance hiver velvet revival",
        "tweed veste tweed automne hiver tendance",
        "sequins paillettes tendance soirée et jour",
        "organza transparence légèreté matière tendance été",
    ],
}


GENERIC_TITLES = [
    "gorpcore tendance mode outdoor urbaine",
    "quiet luxury minimalisme luxueux tendance",
    "robe satin tendance collection",
    "old money aesthetic chic discret",
    "ballet flats ballerines tendance",
    "oversized blazer veste XXL tendance",
    "samba adidas sneaker tendance",
    "crochet top bohème tendance été",
    "wide leg pantalon large tendance",
    "mob wife aesthetic glamour tendance",
    "leopard print imprimé léopard tendance",
    "slip dress robe nuisette tendance",
    "kitten heels talons chaton tendance",
    "denim on denim total look jean",
    "maxi jupe longue tendance bohème",
    "capsule wardrobe essentiels mode",
    "coquette aesthetic ultra-féminin tendance",
    "cargo pants pantalon cargo tendance",
    "barbiecore rose tendance mode",
    "Y2K revival années 2000 tendance",
]


def media_articles(source: str) -> list[dict[str, object]]:
    titles = MEDIA_FIXTURES.get(source, GENERIC_TITLES)
    rows = []
    for index, title in enumerate(titles):
        rows.append(
            {
                "source": source,
                "title": title,
                "url": f"https://fixture.local/{source}/{index}",
                "published_at": FIXTURE_AT,
                "collected_at": FIXTURE_AT,
                "source_is_fixture": True,
            }
        )
    return rows


def reddit_posts(keywords: list[str] | None = None) -> list[dict[str, object]]:
    selected = keywords or KEYWORDS
    posts: list[dict[str, object]] = []
    subreddits = ["fashion", "streetwear", "sneakers", "femalefashionadvice", "malefashionadvice"]
    for keyword_index, keyword in enumerate(selected[:10]):
        for post_index in range(100):
            posts.append(
                {
                    "keyword": keyword,
                    "subreddit": subreddits[(keyword_index + post_index) % len(subreddits)],
                    "post_id": f"fixture-reddit-{keyword_index}-{post_index}",
                    "title": f"{keyword} discussion and styling ideas",
                    "score": 90 - keyword_index * 5 + post_index * 3,
                    "num_comments": 34 - keyword_index + post_index,
                    "created_utc": 1780416000 - keyword_index * 86400 - post_index * 3600,
                    "permalink": f"/r/fashion/comments/{keyword_index}{post_index}/fixture",
                    "collected_at": FIXTURE_AT,
                    "source": "reddit",
                    "source_is_fixture": True,
                }
            )
    return posts


def google_trends(keywords: list[str] | None = None) -> list[dict[str, object]]:
   
    import math
    selected = keywords or KEYWORDS
    start = datetime(2021, 1, 4, tzinfo=timezone.utc)  
    records: list[dict[str, object]] = []
    
    seasonal_keywords = {
        "trench coat": (70, 30, 2),       
        "mocassins": (65, 25, 2),
        "ballet flats": (60, 30, 2),
        "sac baguette": (55, 20, 1),
        "jean brut": (75, 15, 0),
        "veste en cuir": (65, 25, 3),
        "jupe satin": (58, 28, 2),
        "cardigan": (70, 30, 3),         
        "samba adidas": (80, 20, 1),
        "leopard print": (55, 25, 2),
    }
    for keyword_index, keyword in enumerate(selected[:16]):
        base, amplitude, phase = seasonal_keywords.get(keyword, (60 - keyword_index * 3, 20, 1))
        base = max(20, base)
        for week in range(261):  
            date = start + timedelta(weeks=week)
            
            seasonal = amplitude * math.cos(2 * math.pi * (week / 52.0 - phase / 4.0))
           
            trend = (week / 261.0) * 15
           
            noise = 5 * math.sin(week * 0.7 + keyword_index * 1.3)
            value = max(1, min(100, int(base + seasonal + trend + noise)))
            records.append(
                {
                    "keyword": keyword,
                    "date": date.date().isoformat(),
                    "interest": value,
                    "collected_at": FIXTURE_AT,
                    "source": "google_trends",
                    "source_is_fixture": True,
                }
            )
    return records


def streetwear_media_articles(keywords: list[str] | None = None) -> list[dict[str, object]]:
    selected = keywords or KEYWORDS
    articles: list[dict[str, object]] = []
    titles = [
        "{} drops on Hypebeast",
        "{} streetwear trend report",
        "{} culture editorial",
        "{} lookbook feature",
        "{} brand collaboration",
    ]
    feeds = ("hypebeast", "highsnobiety")
    for keyword_index, keyword in enumerate(selected[:10]):
        for article_index in range(100):
            feed_source = feeds[article_index % len(feeds)]
            articles.append(
                {
                    "keyword": keyword,
                    "article_id": f"fixture-streetwear-{keyword_index}-{article_index}",
                    "title": titles[article_index % len(titles)].format(keyword),
                    "description": f"Streetwear coverage for {keyword}",
                    "link": f"https://{feed_source}.com/article/fixture-{keyword_index}-{article_index}",
                    "feed_source": feed_source,
                    "collected_at": FIXTURE_AT,
                    "source": "streetwear_media",
                    "source_is_fixture": True,
                }
            )
    return articles


_BRANDS_BY_KEYWORD: dict[str, list[str]] = {
    "trench coat":   ["Burberry", "Zara", "Mango", "H&M", "Sandro"],
    "mocassins":     ["Gucci", "Mango", "Zara", "Minelli", "André"],
    "ballet flats":  ["Repetto", "Zara", "Mango", "H&M", "Chanel"],
    "sac baguette":  ["Fendi", "Zara", "Mango", "Sandro", "Maje"],
    "jean brut":     ["Levi's", "Acne Studios", "Zara", "H&M", "Weekday"],
    "veste en cuir": ["Zara", "Mango", "ASOS", "Sandro", "The Kooples"],
    "jupe satin":    ["Zara", "H&M", "Mango", "Sandro", "Maje"],
    "cardigan":      ["Sézane", "Zara", "H&M", "Mango", "Uniqlo"],
    "samba adidas":  ["Adidas", "Adidas Originals", "Foot Locker", "JD Sports", "SNKRS"],
    "leopard print": ["Zara", "H&M", "Mango", "ASOS", "Topshop"],
    "mary jane":     ["Repetto", "Zara", "Mango", "André", "Minelli"],
    "quiet luxury":  ["The Row", "Toteme", "Sézane", "Uniqlo", "Arket"],
    "tailoring":     ["Sandro", "Maje", "Zara", "Mango", "& Other Stories"],
    "denim jacket":  ["Levi's", "Zara", "H&M", "ASOS", "Weekday"],
    "mesh flats":    ["Zara", "Mango", "H&M", "ASOS", "Topshop"],
    "kitten heels":  ["Mango", "Zara", "H&M", "Minelli", "André"],
}
_DEFAULT_BRANDS = ["Zara", "H&M", "Mango", "ASOS", "Sandro"]


def vinted_offers(keywords: list[str] | None = None, limit_per_keyword: int = 50) -> list[dict[str, object]]:
    """Fixtures Vinted — aucune API publique officielle disponible.

    Génère des annonces de seconde main réalistes avec des prix plus bas qu'eBay
    (Vinted est orienté particulier-à-particulier). Toutes les entrées sont
    explicitement marquées source_is_fixture=True.
    """
    selected = keywords or KEYWORDS
    offers: list[dict[str, object]] = []
    conditions = ["Très bon état", "Bon état", "Neuf avec étiquettes", "Bon état", "Satisfaisant"]
    sizes = ["XS", "S", "M", "L", "XL"]
    for keyword_index, keyword in enumerate(selected[:10]):
        brands = _BRANDS_BY_KEYWORD.get(keyword, _DEFAULT_BRANDS)
        for offer_index in range(min(limit_per_keyword, 50)):
            
            price = round(8 + keyword_index * 3 + offer_index * 1.8, 2)
            brand = brands[offer_index % len(brands)]
            offers.append(
                {
                    "keyword": keyword,
                    "item_id": f"fixture-vinted-{keyword_index}-{offer_index}",
                    "title": f"{brand} {keyword.title()} taille {sizes[offer_index % len(sizes)]}",
                    "brand": brand,
                    "price_value": price,
                    "currency": "EUR",
                    "condition": conditions[offer_index % len(conditions)],
                    "item_url": f"https://www.vinted.fr/vetements/{keyword_index}-{offer_index}",
                    "seller_username": f"vinted_user_{offer_index}",
                    "shipping_included": offer_index % 3 != 0,
                    "collected_at": FIXTURE_AT,
                    "source": "vinted",
                    "source_is_fixture": True,
                }
            )
    return offers


def marktplaats_offers(keywords: list[str] | None = None, limit_per_keyword: int = 50) -> list[dict[str, object]]:
    selected = keywords or KEYWORDS
    offers: list[dict[str, object]] = []
    conditions = ["Zo goed als nieuw", "Gebruikt", "Nieuw"]
    for keyword_index, keyword in enumerate(selected[:10]):
        brands = _BRANDS_BY_KEYWORD.get(keyword, _DEFAULT_BRANDS)
        for offer_index in range(min(limit_per_keyword, 50)):
            price = round(12 + keyword_index * 4 + offer_index * 2.2, 2)
            brand = brands[offer_index % len(brands)]
            offers.append(
                {
                    "keyword": keyword,
                    "item_id": f"fixture-marktplaats-{keyword_index}-{offer_index}",
                    "title": f"{brand} {keyword.title()} maat {['36','38','40','42','44'][offer_index % 5]}",
                    "brand": brand,
                    "price_value": price,
                    "currency": "EUR",
                    "condition": conditions[offer_index % len(conditions)],
                    "price_type": "FIXED",
                    "item_url": f"https://www.marktplaats.nl/v/fixture/{keyword_index}-{offer_index}",
                    "seller_username": f"marktplaats_user_{offer_index}",
                    "seller_feedback_score": 0,
                    "seller_feedback_percentage": 0.0,
                    "shipping_cost": 0.0,
                    "marketplace_id": "MARKTPLAATS_NL",
                    "collected_at": FIXTURE_AT,
                    "source": "marktplaats",
                    "source_is_fixture": True,
                }
            )
    return offers


def ebay_offers(keywords: list[str] | None = None) -> list[dict[str, object]]:
    selected = keywords or KEYWORDS
    offers: list[dict[str, object]] = []
    conditions = ["Neuf", "Occasion", "Tres bon etat"]
    for keyword_index, keyword in enumerate(selected[:10]):
        brands = _BRANDS_BY_KEYWORD.get(keyword, _DEFAULT_BRANDS)
        for offer_index in range(200):
            price = round(35 + keyword_index * 7 + offer_index * 4.5, 2)
            brand = brands[offer_index % len(brands)]
            offers.append(
                {
                    "keyword": keyword,
                    "item_id": f"fixture-ebay-{keyword_index}-{offer_index}",
                    "title": f"{brand} {keyword.title()} - {['XS','S','M','L','XL'][offer_index % 5]}",
                    "brand": brand,
                    "price_value": price,
                    "currency": "EUR",
                    "condition": conditions[offer_index % len(conditions)],
                    "item_url": f"https://www.ebay.fr/itm/fixture-{keyword_index}-{offer_index}",
                    "seller_username": f"seller_{brand.lower().replace(' ', '_').replace(chr(39), '')}",
                    "seller_feedback_score": 500 + keyword_index * 120 - (offer_index % 50) * 10,
                    "seller_feedback_percentage": 98.5 - (offer_index % 10) * 0.4,
                    "shipping_cost": round(4.9 + (offer_index % 10) * 0.5, 2),
                    "marketplace_id": "EBAY_FR",
                    "collected_at": FIXTURE_AT,
                    "source": "ebay",
                    "source_is_fixture": True,
                }
            )
    return offers

