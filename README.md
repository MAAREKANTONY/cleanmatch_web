# CleanMatch Web - Iteration 5

Cette itération ajoute la robustesse des jobs :
- bouton **Kill job** dans l'UI
- champ `last_heartbeat`
- détection et auto-fail des jobs stale via **Celery Beat**
- garde-fou sur l'espace disque avant lancement et pendant le traitement
- meilleure gestion des annulations côté worker

## Démarrage

```bash
cp .env.example .env
docker compose up --build
```

## Git init

```bash
git init
git add .
git commit -m "iteration 5 - job robustness"
```

## URLs

- http://localhost:8080/
- http://localhost:8080/jobs/new/
- http://localhost:8080/health/

## Notes importantes

- le bouton **Kill job** annule proprement les jobs `queued`
- pour les jobs `running`, l'arrêt s'effectue au prochain checkpoint worker (progress/log/check disque)
- le service `beat` marque automatiquement en `failed` les jobs `running` sans heartbeat depuis trop longtemps
- tu peux ajuster les seuils dans `.env`


## Environment

Use `.env.example` as the base for `.env`. This iteration keeps compatibility with the previous variable names (`DEBUG`, `SECRET_KEY`, `ALLOWED_HOSTS`, `TIME_ZONE`) and also still accepts the newer `DJANGO_*` aliases.
