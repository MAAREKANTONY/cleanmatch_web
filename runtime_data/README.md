# Runtime data

Ce dossier est monté dans les conteneurs sous `/data`.

Utilisation recommandée :
- conserver ici les fichiers persistants qui ne doivent pas être écrasés par une nouvelle livraison
- placer les overrides métier dans `config_overrides/`

Exemples :
- `runtime_data/config_overrides/ai_review/llm_providers.yaml`
- `runtime_data/config_overrides/marketsegmenter/countries/it.yaml`

Initialisation rapide :

```bash
docker compose exec web python manage.py bootstrap_config_overrides
```

Après modification d'un YAML/CSV override, redémarrer `web`, `worker` et `beat` pour recharger la configuration en mémoire.
