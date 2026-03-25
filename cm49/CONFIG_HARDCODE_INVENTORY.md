# CleanMatch Web — inventaire des configurations hardcodées

## Objectif

Ce document inventorie les principales configurations hardcodées identifiées dans l'itération 24, et précise leur cible d'externalisation dans le nouveau `app/config_catalog/` livré en itération 25.

## Légende

- **Statut**
  - `externalisée` : la configuration est désormais déplacée dans `app/config_catalog/` et chargée par le code.
  - `inventoriée` : la configuration est documentée ici mais reste encore dans le code pour limiter les risques de régression dans cette itération.
- **Priorité**
  - `P1` : métier critique, doit rester facilement modifiable.
  - `P2` : technique importante.
  - `P3` : plateforme / maintenance.

## Tableau d'inventaire

| Module | Configuration actuelle | Nature | Fichier cible | Statut | Priorité |
|---|---|---|---|---|---|
| Normalizer | `EUROPE_COUNTRY_CHOICES` | pays/métier | `config_catalog/normalizer/mapping_fields.yaml` | externalisée | P1 |
| Normalizer | `COUNTRY_NAME_TO_CODE` | pays/métier | `config_catalog/normalizer/mapping_fields.yaml` | externalisée | P1 |
| Normalizer | `COUNTRY_PROFILES` | pays/métier | `config_catalog/normalizer/countries/*.yaml` | externalisée | P1 |
| Normalizer | `DEFAULT_COUNTRY_CODE` | pays/métier | `config_catalog/normalizer/mapping_fields.yaml` | externalisée | P1 |
| Normalizer | `SUPPORTED_COUNTRY_CODES` | pays/métier | `config_catalog/normalizer/mapping_fields.yaml` | externalisée | P1 |
| Normalizer | `CANONICAL_MAPPING_FIELDS` | mapping | `config_catalog/normalizer/mapping_fields.yaml` | externalisée | P1 |
| Normalizer | `REQUIRED_MATCHCODE_FIELDS` | mapping | `config_catalog/normalizer/mapping_fields.yaml` | externalisée | P1 |
| Normalizer | `COLUMN_ALIASES` | mapping | `config_catalog/normalizer/column_aliases.yaml` | externalisée | P1 |
| Normalizer | `REFERENCE_COLUMNS` | output | `config_catalog/normalizer/output_columns.yaml` | externalisée | P2 |
| Normalizer | `COLUMNS_TO_KEEP` | output | `config_catalog/normalizer/output_columns.yaml` | externalisée | P2 |
| Normalizer | `PREFERRED_OUTPUT_ORDER` | output | `config_catalog/normalizer/output_columns.yaml` | externalisée | P2 |
| Normalizer | `COUNTRY_STOPWORD_PATTERNS` | dérivée des profils pays | calculé depuis `countries/*.yaml` | externalisée | P1 |
| Matcher | `MATCHER_MAPPING_FIELDS` | mapping | `config_catalog/matcher/mapping_fields.yaml` | externalisée | P1 |
| Matcher | `MATCHER_REQUIRED_FIELDS` | mapping | `config_catalog/matcher/mapping_fields.yaml` | externalisée | P1 |
| Matcher | `MATCHER_COLUMN_ALIASES` | mapping | `config_catalog/matcher/column_aliases.yaml` | externalisée | P1 |
| Matcher | `COMMON_STOP_WORDS` | métier | `config_catalog/matcher/stopwords.yaml` | externalisée | P1 |
| Matcher | `LEGAL_STOP_WORDS` | métier | `config_catalog/matcher/stopwords.yaml` | externalisée | P1 |
| Matcher | `AUTO_REASONS` | UX/métier | `config_catalog/matcher/auto_reasons.yaml` | externalisée | P2 |
| Matcher | seuils par défaut (`threshold_name`, `threshold_voie`, `top_k_per_master`, `threshold_phone_review`) | scoring | `config_catalog/matcher/thresholds.yaml` | externalisée | P1 |
| Matcher | poids de score et review heuristics internes | scoring | futur `config_catalog/matcher/scoring.yaml` | inventoriée | P1 |
| Geocoder | `GEOCODER_MAPPING_FIELDS` | mapping | `config_catalog/geocoder/mapping_fields.yaml` | externalisée | P1 |
| Geocoder | `GEOCODER_REQUIRED_FIELDS` | mapping | `config_catalog/geocoder/mapping_fields.yaml` | externalisée | P1 |
| Geocoder | `GEOCODER_COLUMN_ALIASES` | mapping | `config_catalog/geocoder/column_aliases.yaml` | externalisée | P1 |
| Geocoder | alias pays de `_clean_country()` | pays | `config_catalog/geocoder/countries.yaml` | externalisée | P1 |
| Geocoder | provider par défaut / `user_agent` / timeout / rate limit | technique | `config_catalog/geocoder/providers.yaml` | externalisée | P2 |
| Geocoder | nom cache SQLite | technique | `config_catalog/geocoder/cache.yaml` | externalisée | P2 |
| Geocoder | checkpoint (`checkpoint_every`, `resume_enabled`, suffixe) | technique | `config_catalog/geocoder/checkpoint.yaml` | externalisée | P2 |
| Geocoder | ordre de requête `_full_query()` | technique | `config_catalog/geocoder/query_templates.yaml` | externalisée | P2 |
| Geoclass | `GEOCLASS_MAPPING_FIELDS` | mapping | `config_catalog/geoclass/mapping_fields.yaml` | externalisée | P1 |
| Geoclass | `GEOCLASS_REQUIRED_FIELDS` | mapping | `config_catalog/geoclass/mapping_fields.yaml` | externalisée | P1 |
| Geoclass | `KEYWORD_RULES` | métier | `config_catalog/geoclass/keyword_rules.yaml` | externalisée | P1 |
| Market Segmenter | `MARKETSEGMENTER_MAPPING_FIELDS` | mapping | `config_catalog/marketsegmenter/mapping_fields.yaml` | externalisée | P1 |
| Market Segmenter | `MARKETSEGMENTER_REQUIRED_FIELDS` | mapping | `config_catalog/marketsegmenter/mapping_fields.yaml` | externalisée | P1 |
| Market Segmenter | `MARKETSEGMENTER_COLUMN_ALIASES` | mapping | `config_catalog/marketsegmenter/column_aliases.yaml` | externalisée | P1 |
| Market Segmenter | `COUNTRY_LANGS` | pays/langues | `config_catalog/marketsegmenter/country_langs.yaml` | externalisée | P1 |
| Market Segmenter | `COUNTRY_NAME_TO_CODE` | pays | `config_catalog/marketsegmenter/country_name_to_code.yaml` | externalisée | P1 |
| Market Segmenter | `ALL_TEXT_FIELDS` | métier | `config_catalog/marketsegmenter/mapping_fields.yaml` | externalisée | P2 |
| Market Segmenter | `DEBUG_OUTPUT_COLUMNS` | output/debug | `config_catalog/marketsegmenter/mapping_fields.yaml` | externalisée | P2 |
| Market Segmenter | `BASE_FAMILY_SCORE_RULES` | scoring | `config_catalog/marketsegmenter/family_scoring.yaml` | externalisée | P1 |
| Market Segmenter | `PRICE_BUCKETS` | scoring | `config_catalog/marketsegmenter/price_rules.yaml` | externalisée | P1 |
| Market Segmenter | `PRICE_SCORE_RULES` | scoring | `config_catalog/marketsegmenter/price_rules.yaml` | externalisée | P1 |
| Market Segmenter | `KEYWORD_RULES` | métier global | `config_catalog/marketsegmenter/global_keywords.yaml` | externalisée | P1 |
| Market Segmenter | `COUNTRY_KEYWORD_RULES` | métier pays | `config_catalog/marketsegmenter/countries/*.yaml` | externalisée | P1 |
| Market Segmenter | `NEGATIVE_KEYWORDS` | métier | `config_catalog/marketsegmenter/negative_keywords.yaml` | externalisée | P1 |
| Market Segmenter | `google_type_mapping_proposed.csv` | mapping volumineux | `config_catalog/marketsegmenter/type_mapping.csv` | externalisée | P1 |
| Jobs / plateforme | suffixes de fichiers de sortie | technique | futur `config_catalog/platform/files.yaml` | inventoriée | P3 |
| Jobs / plateforme | time limits Celery | technique | futur `config_catalog/platform/celery.yaml` | inventoriée | P3 |
| Jobs / plateforme | stale thresholds / cleanup defaults | technique | futur `config_catalog/platform/jobs.yaml` | inventoriée | P3 |
| Settings | `LANGUAGE_CODE`, time limits, schedule maintenance | plateforme | futur `config_catalog/platform/*.yaml` ou `.env` | inventoriée | P3 |

## Ce qui est livré dans l'itération 25

- un nouveau dossier `app/config_catalog/`
- un `config_loader.py`
- un `config_validator.py`
- externalisation effective des principales configurations des modules :
  - `normalizer`
  - `matcher` (partie mapping / stopwords / seuils par défaut)
  - `geocoder`
  - `geoclass`
  - `marketsegmenter`

## Ce qui reste volontairement à refactorer ensuite

Pour limiter les risques de régression dans cette itération, certaines règles **internes** restent encore dans le code et devront être sorties dans un sprint suivant :

1. **Matcher scoring deep rules**
   - pondérations détaillées de `composite_score`
   - règles d'automatch
   - règles de `review_status`

2. **Normalizer parsing heuristics fines**
   - quelques comportements procéduraux de parsing d'adresse encore mêlés au code

3. **Plateforme / jobs**
   - délais de stale jobs
   - politiques de cleanup
   - certains paramètres Celery / maintenance

## Recommandation de suite

### Sprint suivant recommandé
- externaliser le **scoring détaillé du Matcher**
- externaliser les **paramètres plateforme**
- ajouter une **commande Django de validation du catalogue**
- ajouter des **tests de non-régression de chargement config**
