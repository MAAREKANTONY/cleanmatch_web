# CleanMatch Web — Itération 4

Cette itération améliore principalement l'interface utilisateur et le confort d'exploitation.

## Nouveautés

- dashboard jobs plus lisible avec KPI et filtres rapides
- page de détail job avec rafraîchissement via API sans reload complet
- page de création plus guidée
- inspection d'un fichier Excel avant lancement du Normalizer
- préremplissage assisté du nom d'onglet grâce à la liste des sheets détectées

## Démarrage

```bash
cp .env.example .env
```

Si le projet n'est pas encore versionné :

```bash
git init
git add .
git commit -m "iteration 4 - dashboard ui and excel inspection"
```

Lancement :

```bash
docker compose up --build
```

Puis au besoin :

```bash
docker compose exec web python manage.py migrate
docker compose exec web python manage.py createsuperuser
```

## URLs utiles

- http://localhost:8080/
- http://localhost:8080/jobs/new/
- http://localhost:8080/health/

## Note

L'inspection Excel est utilisée uniquement côté UI pour aider au choix de l'onglet avant soumission du job Normalizer. Le mapping interactif des colonnes reste prévu pour l'itération suivante.
