# CleanMatch Web - Iteration 6

Cette itération ajoute au **Normalizer** :
- l'analyse de structure du fichier Excel
- la détection d'onglets et d'entête probable
- la détection des colonnes visibles
- des suggestions automatiques de mapping
- un mapping de colonnes persistant dans le job
- l'application du mapping côté moteur métier avant nettoyage et matchcode

## Démarrage

```bash
cp .env.example .env
docker compose up --build
```

## Git init

```bash
git init
git add .
git commit -m "iteration 6 - normalizer structure analysis and column mapping"
```

## URLs

- http://localhost:8080/
- http://localhost:8080/jobs/new/
- http://localhost:8080/health/

## Notes importantes

- le normalizer continue d'écrire les résultats en **CSV UTF-8 avec BOM**
- pour générer les colonnes de matchcode, il faut mapper au minimum : `address`, `zipcode`, `city`
- le mapping choisi est stocké dans `parameters_json` du job
- la robustesse des jobs de l'itération 5 reste incluse : kill job, heartbeat, stale monitoring

## Environment

Use `.env.example` as the base for `.env`. This iteration keeps compatibility with the previous variable names (`DEBUG`, `SECRET_KEY`, `ALLOWED_HOSTS`, `TIME_ZONE`) and also still accepts the newer `DJANGO_*` aliases.


## Iteration 8

- Matcher V1 with master/slave inspection
- Mapping suggestions for both datasets
- CSV output for match results


## Iteration 12 — Cleaning & maintenance

Nouveautés :
- suppression d’un job terminé depuis l’UI
- suppression des fichiers input / output / error d’un job
- purge des jobs passés depuis le dashboard
- purge des fichiers orphelins

Commandes utiles :

```bash
docker compose exec web python manage.py cleanup_jobs --days 30
docker compose exec web python manage.py cleanup_files
```


## Iteration 13

- Matcher V3 / parity audit
- `diagnostics.csv` ajouté dans le ZIP matcher
- statistiques supplémentaires dans `summary.json`
- tag UI mis à jour vers `Itération 13 — Matcher Parity Audit`


## Iteration 14 — Normalizer Multi-country Europe V1
- profils pays: FR, IT, ES, DE, BE, NL, GB, PT
- normalisation address / postcode / legal_id
- nouveau champ `country_code` dans le flow normalizer
