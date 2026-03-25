# CleanMatch Web - Iteration 38

Cette itération ajoute une couche propre de **configuration persistante** :

- catalogue par défaut versionné dans `app/config_catalog/`
- overrides persistants lus depuis `CONFIG_OVERRIDE_DIR`
- bootstrap des overrides via une commande Django
- variables LLM documentées dans `.env.example`

## Démarrage

```bash
cp .env.example .env
mkdir -p runtime_data/config_overrides
docker compose up --build
```

## Bootstrap des overrides persistants

```bash
docker compose exec web python manage.py bootstrap_config_overrides
```

Cette commande copie le catalogue par défaut dans `CONFIG_OVERRIDE_DIR` sans écraser les fichiers déjà présents.

## Emplacement recommandé

Dans `.env`, vous pouvez garder les chemins par défaut :

```env
CONFIG_CATALOG_DIR=/app/config_catalog
CONFIG_OVERRIDE_DIR=/data/config_overrides
HOST_RUNTIME_DATA_DIR=./runtime_data
```

Pour une production plus robuste, vous pouvez aussi pointer `HOST_RUNTIME_DATA_DIR` vers un dossier externe au répertoire livré, par exemple :

```env
HOST_RUNTIME_DATA_DIR=/opt/cleanmatch_runtime
```

## Règle de gouvernance

- `.env` : secrets, flags runtime, chemins, clés API
- `app/config_catalog/` : configuration métier par défaut livrée avec le code
- `CONFIG_OVERRIDE_DIR` : configuration métier locale et persistante

## LLM

Les clés API doivent être définies dans `.env` :

```env
OPENAI_API_KEY=
GEMINI_API_KEY=
ANTHROPIC_API_KEY=
```

Le YAML `app/config_catalog/ai_review/llm_providers.yaml` ne doit contenir que le **nom** de la variable d'environnement, jamais la clé brute.

## Rechargement de configuration

Après modification d'un fichier YAML/CSV override, redémarrer `web`, `worker` et `beat` pour recharger la configuration en mémoire.


## Diagnostic LLM

Pour inspecter la configuration LLM réellement chargée dans le conteneur :

```bash
docker compose exec worker python manage.py inspect_llm_runtime --provider anthropic_messages_json --model claude-sonnet-4-6
```

Cette commande affiche :
- le fichier `llm_providers.yaml` réellement utilisé
- son origine (`default` ou `override`)
- la variable d'environnement attendue
- si la clé API est réellement visible
- si le modèle demandé appartient bien au provider

En plus, l'AI Review écrit maintenant dans les logs worker un diagnostic JSON complet de l'état runtime LLM.


## Iteration 39

AI Review LLM output semantics were clarified to distinguish job-level configuration from row-level execution. Output now includes `ai_llm_configured`, `ai_llm_live_ready`, `ai_llm_attempted`, and `ai_llm_result_source`, and fallback rows no longer claim that no provider was enabled when the provider was actually configured.


## Iteration 44

- Added a dedicated `app/bigquery/client.py` service module.
- BigQuery country filter now injects `@country_code` only when a country code is provided.
- BigQuery preview now returns the executed SQL preview.
