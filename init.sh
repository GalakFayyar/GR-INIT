#!/bin/bash
#
#	Script d'initialisation de l'environnement
#	Lance les scripts Python dans l'environnement virtual adapté
#	Charge les données dans Elasticsearch avec les données dans 
#	le dossier init/
#

DATA_PATH=/home/tfalcher/devs/GeoRequetes/georequetes-init/data
PEW_ENV=p3.4_new
PARAM=init # init ou update

# Import initial des données :
# Chargement des données issues des fichiers dans l'index ES

# pew in $PEW_ENV python init_data.py --type_doc=communes_pj --source_file=$DATA_PATH/communes_pj.csv --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=communes --source_file=$DATA_PATH/communes.geojson --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=departements --source_file=$DATA_PATH/departements.geojson --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=regions --source_file=$DATA_PATH/regions.geojson --$PARAM

pew in $PEW_ENV python init_data.py --type_doc=requetes --$PARAM