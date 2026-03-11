# Suivi pluviometrie LGV

Application Streamlit locale pour consolider pluie, hydro et vigilance autour de la LGV a partir des referentiels SIG internes et de plusieurs fournisseurs externes.

## Perimetre

- Tableau de bord exploitation
- Vue lineaire par troncon PK
- Vue communes traversees
- Vue stations sources
- Historique d'episodes pluvieux
- Stockage local DuckDB
- Refresh quotidien ou complet

## Referentiels attendus

- `communes_pk_lgv.xlsx`
- `LRS_AXES.gpkg`
- `LRS_PK.gpkg`

Les chemins par defaut sont portes par [config.yaml](./config.yaml) et peuvent etre surcharges par variables d'environnement.

## Variables d'environnement utiles

- `METEOFRANCE_API_KEY`
- `DATA_ROOT`
- `COMMUNES_XLSX_PATH`
- `LRS_AXES_PATH`
- `LRS_PK_PATH`
- `OPEN_METEO_ENABLED`
- `METEOFRANCE_ENABLED`
- `HUBEAU_ENABLED`
- `VIGICRUES_ENABLED`
- `HYDROPORTAIL_ENABLED`
- `SANDRE_ENABLED`

Sur `Streamlit Community Cloud`, la cle API peut etre definie dans `Secrets`.

## Demarrage

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## Refresh des donnees

Refresh quotidien :

```powershell
python refresh.py --daily
```

Refresh complet :

```powershell
python refresh.py --full
```

## Deploiement Streamlit Community Cloud

1. Publier ce dossier dans un repo GitHub.
2. Dans Streamlit Community Cloud, creer une nouvelle app depuis ce repo.
3. Utiliser `app.py` comme fichier principal.
4. Dans `Settings > Secrets`, ajouter au minimum :

```toml
METEOFRANCE_API_KEY = "ta-cle"
```

5. Redemarrer l'application.

### Referentiels SIG sur le cloud

`Streamlit Community Cloud` ne peut pas lire les chemins locaux `C:\...`.

Deux options :

- charger `communes_pk_lgv.xlsx` et `LRS_PK.gpkg` directement dans la barre laterale de l'application
- ou monter des fichiers externes et renseigner leurs chemins dans `Secrets`

Sans ces referentiels, l'application demarre en mode demonstration.

## Structure du projet

- `app.py` : page principale Streamlit
- `pages/` : vues PK, communes, stations et historique
- `lgv_pluvio/connectors/` : connecteurs fournisseurs
- `lgv_pluvio/data_pipeline/` : chargement referentiels, matching spatial, refresh
- `lgv_pluvio/domain/` : logique de vigilance
- `lgv_pluvio/storage/` : persistance DuckDB
- `config.yaml` : configuration centrale

## Notes d'implementation

- En absence de referentiels lisibles ou de donnees chargees, l'application injecte un jeu de demonstration pour rester navigable.
- `Meteo-France` est gere comme fournisseur prioritaire, avec `Open-Meteo` comme fallback.
- `Hub'Eau`, `Vigicrues`, `HydroPortail` et `Sandre` sont integres via des connecteurs dedies, avec gestion des erreurs et journal de synchronisation.
