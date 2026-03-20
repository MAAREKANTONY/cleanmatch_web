# CleanMatch Config Catalog

Ce dossier contient la configuration métier externalisée de CleanMatch Web.

## Principe

- le code Python exécute les algorithmes
- les fichiers YAML/CSV portent les règles métier et les paramètres modifiables

## Structure

- `version.yaml` : version du catalogue
- `normalizer/` : profils pays, alias colonnes, ordre des sorties
- `matcher/` : alias, stopwords, seuils par défaut
- `geocoder/` : provider, cache, checkpoint, ordre de requête
- `geoclass/` : règles heuristiques
- `marketsegmenter/` : langues pays, mots-clés, prix, scoring, mapping Google types

## Règle de gestion

Toute nouvelle règle métier durable doit être préférentiellement ajoutée ici plutôt qu'en dur dans les services Python.
