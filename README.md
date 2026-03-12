# CleanMatch Web — Itération 2

Cette itération ajoute le pipeline complet :
- upload de fichier
- création d'un job Django
- exécution asynchrone via Celery
- progression/logs
- génération d'un fichier résultat téléchargeable

## Démarrage

```bash
docker compose up --build
```

Puis dans un second terminal :

```bash
docker compose exec web python manage.py migrate
docker compose exec web python manage.py createsuperuser
```

## URLs utiles

- Application : `http://localhost/`
- Nouveau job : `http://localhost/jobs/new/`
- Admin : `http://localhost/admin/`
- Health : `http://localhost/health/`

## Logs

```bash
docker compose logs -f web
docker compose logs -f worker
```

## Git

```bash
git init
git add .
git commit -m "iteration 2 - upload and async job pipeline"
```

## Notes

Le traitement métier réel n'est pas encore branché. Le worker génère un résultat stub à partir du fichier uploadé afin de valider la chaîne technique de bout en bout.
