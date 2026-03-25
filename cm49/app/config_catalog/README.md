# CleanMatch Config Catalog

Ce dossier contient la configuration métier **par défaut** de CleanMatch Web.

## Principe

- le code Python exécute les algorithmes
- les fichiers YAML/CSV portent les règles métier et les paramètres modifiables
- les overrides persistants doivent vivre dans `CONFIG_OVERRIDE_DIR`

## Priorité de lecture

Pour un chemin relatif donné, CleanMatch cherche dans cet ordre :

1. `CONFIG_OVERRIDE_DIR/<relative_path>`
2. `CONFIG_CATALOG_DIR/<relative_path>`

Ainsi, un fichier override ne sera pas écrasé par une nouvelle livraison du code.

## Structure

- `version.yaml` : version du catalogue
- `normalizer/` : profils pays, alias colonnes, ordre des sorties
- `matcher/` : alias, stopwords, seuils par défaut
- `geocoder/` : provider, cache, checkpoint, ordre de requête
- `geoclass/` : règles heuristiques
- `marketsegmenter/` : langues pays, mots-clés, prix, scoring, mapping Google types
- `ai_review/` : mapping, capacités, guardrails, providers LLM

## Règle de gestion

Toute nouvelle règle métier durable doit être préférentiellement ajoutée ici plutôt qu'en dur dans les services Python.

## Secrets

Les secrets ne doivent jamais être stockés dans les YAML/CSV du catalogue.

Exemple attendu :
- dans `llm_providers.yaml` : `api_key_env: ANTHROPIC_API_KEY`
- dans `.env` : `ANTHROPIC_API_KEY=...`
