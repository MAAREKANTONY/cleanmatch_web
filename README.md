# CleanMatch Web — Itération 3

Cette itération branche le premier vrai moteur métier : **Normalizer**.

## Ce qui est inclus

- architecture Docker `Django + PostgreSQL + Redis + Celery + Nginx`
- upload de fichier et création de job
- exécution asynchrone côté worker
- suivi de progression et logs
- **normalizer métier branché** :
  - nettoyage des colonnes
  - génération `num_voie`
  - génération `voie`
  - génération `matchcode`
  - tentative de détection `chaine` via `app/legacy_data/chaines.csv` si présent
- téléchargement du fichier Excel résultat

## Limites connues de cette itération

- le normalizer web supporte uniquement les fichiers Excel `.xlsx/.xlsm/.xltx/.xltm`
- si plusieurs onglets sont présents et qu'aucun nom d'onglet n'est fourni, le premier est utilisé
- le mapping interactif des colonnes n'est **pas encore** migré
- `Matcher` et `Geocoder` restent en stub

## Installation

```bash
cp .env.example .env
```

### Initialisation Git

```bash
git init
git add .
git commit -m "iteration 3 - real normalizer service"
```

### Démarrage

```bash
docker compose up --build
```

Puis dans un second terminal :

```bash
docker compose exec web python manage.py migrate
docker compose exec web python manage.py createsuperuser
```

## URLs utiles

- Application : `http://localhost:8080/`
- Nouveau job : `http://localhost:8080/jobs/new/`
- Admin : `http://localhost:8080/admin/`
- Health : `http://localhost:8080/health/`

## Test conseillé

1. Aller sur `/jobs/new/`
2. Choisir `Normalizer`
3. Uploader un fichier Excel avec au minimum `address`, `zipcode`, `city`
4. Cocher ou décocher les options selon le besoin
5. Lancer le job
6. Télécharger le résultat `.xlsx`

## Données chaînes

Pour réactiver la recherche locale de chaînes, déposer un fichier ici :

```text
app/legacy_data/chaines.csv
```

Colonnes attendues :
- `name`
- `keyword`

## Logs

```bash
docker compose logs -f web
docker compose logs -f worker
```
