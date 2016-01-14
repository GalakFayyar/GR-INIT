#!/usr/bin/env python
"""
    Intègres les données à destination de l'application GeoRequetes dans Elasticsearch

    Usage:
        init_data.py --type_doc=<doc_type> [--source_file=<file_path>] [--init] [--update] [--debug] 

    Example:
        python init_data.py --type_doc=referentiel_activites --source_file=./data/csv/referentiel_activites.csv

    Options:
        --help                      Affiche l'aide
        --type_doc=<doc_type>       Type de document à traiter
        --source_file=<file_path>   Fichier contenant les données à importer ou à mettre à jour
        --init                      Initialise les données pour ce type de documents
        --update                    Met à jour les données pour ce type de documents
"""
from elasticsearch import Elasticsearch, TransportError
from logger import logger, configure
from docopt import docopt
import json, time

from swallow.inout.ESio import ESio
from swallow.inout.CSVio import CSVio
from swallow.inout.JsonFileio import JsonFileio
from swallow.Swallow import Swallow

def file_to_elasticsearch(p_docin, p_type, p_es_conn, p_es_index, p_arguments):
    doc = {}

    if p_type == "communes_pj":
        commune_pj = {
            'code_localite_pj': p_docin[0],
            'code_localite_insee': p_docin[1],
            'code_localite_insee_pj': p_docin[2],
            'libelle': p_docin[3],
            'principale': True if p_docin[4] == "1" else False,
        }

        doc = [{
            "_id": p_docin[1],
            "_type": p_type,
            "_source": commune_pj
        }]

        return doc

    if p_type == "communes":
        tab_communes =  []
        for commune in p_docin['features']:
            code_commune = commune['properties']['code']
            # Enrichissement de la commune avec le code localite Pages Jaunes
            try:
                es_doc_commune_pj = p_es_conn.get(id=code_commune, doc_type='communes_pj', index=p_es_index)
            except TransportError as e:
                logger.info("Commune %s non présente dans le référentiel communes Pages Jaunes", code_commune)
            else:
                if es_doc_commune_pj and len(es_doc_commune_pj) > 0:
                    code_localite_pj = es_doc_commune_pj['_source']['code_localite_pj']
                    commune['properties']['code_pj'] = code_localite_pj

                    tab_communes.append({
                        "_id": code_localite_pj,
                        "_type": p_type,
                        "_source": commune
                    })
                else:
                    logger.info("Code commune %s erroné dans le référentiel communes Pages Jaunes", code_commune)

        return tab_communes

    elif p_type == "regions":
        tab_regions =  []
        for region in p_docin['features']:
            tab_regions.append({
                "_id": 'R' + region['properties']['code'],
                "_type": p_type,
                "_source": region
            })

        return tab_regions

    elif p_type == "departements":
        tab_departements =  []
        for departement in p_docin['features']:
            tab_departements.append({
                "_id": 'D' + departement['properties']['code'],
                "_type": p_type,
                "_source": departement
            })

        return tab_departements

    elif p_type == "requetes":
        doc = p_docin['_source']
        # Si localité : enrichissement de la donnée avec le centroide de la localité
        if 'typegeosimple' in doc and doc['typegeosimple'] == "L":
            try:
                code_commune_pj = doc['idlocalite']
                es_commune = p_es_conn.get(id=code_commune_pj, doc_type='communes', index=p_es_index)
            except TransportError as e:
                logger.info("Commune %s non présente dans le référentiel communes", code_commune_pj)
            else:
                if es_commune and len(es_commune) > 0:
                    doc['position'] = {
                        'lat': es_commune['_source']['properties']['centroide_y'],
                        'lng': es_commune['_source']['properties']['centroide_x']
                    }
                else:
                    logger.error('Erreur lors de la récupération de la commune %s', code_commune_pj)

        returned_doc = {
            "_type": p_type,
            "_source": doc
        }

        return [returned_doc]

def run_import(type_doc = None, source_file = None):
    conf = json.load(open('./init-conf.json'))

    # Command line args
    arguments = docopt(__doc__, version=conf['version'])

    configure(conf['log']['level_values'][conf['log']['level']],
              conf['log']['dir'], 
              conf['log']['filename'],
              conf['log']['max_filesize'], 
              conf['log']['max_files'])

    #
    #   Création du mapping
    # 

    es_mappings = json.load(open('data/es.mappings.json'))

    # Connexion ES métier
    try:
        param = [{'host': conf['connectors']['elasticsearch']['host'],
                  'port': conf['connectors']['elasticsearch']['port']}]
        es = Elasticsearch(param)
        logger.info('Connected to ES Server: %s', json.dumps(param))
    except Exception as e:
        logger.error('Connection failed to ES Server : %s', json.dumps(param))
        logger.error(e)

    # Création de l'index ES metier cible, s'il n'existe pas déjà
    index = conf['connectors']['elasticsearch']['index']
    if not es.indices.exists(index):
        logger.debug("L'index %s n'existe pas : on le crée", index)
        body_create_settings = {
            "settings" : {
                "index" : {
                    "number_of_shards" : conf['connectors']['elasticsearch']['number_of_shards'],
                    "number_of_replicas" : conf['connectors']['elasticsearch']['number_of_replicas']
                },
                "analysis" : {
                    "analyzer": {
                        "lower_keyword": {
                            "type": "custom",
                            "tokenizer": "keyword",
                            "filter": "lowercase"
                        }
                    }
                }
            }
        }
        es.indices.create(index, body=body_create_settings)
        # On doit attendre 5 secondes afin de s'assurer que l'index est créé avant de poursuivre
        time.sleep(2)

        # Création des type mapping ES
        for type_es, properties in es_mappings['georequetes'].items():
            logger.debug("Création du mapping pour le type de doc %s", type_es)
            es.indices.put_mapping(index=index, doc_type=type_es, body=properties)

        time.sleep(2)

    #
    #   Import des données initiales
    #

    # Objet swallow pour la transformation de données
    swal = Swallow()

    # Tentative de récupération des paramètres en argument
    type_doc = arguments['--type_doc'] if not type_doc else type_doc
    source_file = arguments['--source_file'] if not source_file else ('./upload/' + source_file)

    if arguments['--update']:
        if type_doc in ['referentiel_activites', 'referentiel_communes', 'communes', 'activites_connexes']:
            logger.debug("Suppression des documents de type %s", type_doc)
            es.indices.delete_mapping(conf['connectors']['elasticsearch']['index'], type_doc)
            time.sleep(1)
            es.indices.put_mapping(index=conf['connectors']['elasticsearch']['index'], doc_type=type_doc, body=es_mappings['georequetes'][type_doc])
            time.sleep(1)

    if arguments['--init']:
        try:
            logger.debug("Suppression des documents de type %s", type_doc)
            es.indices.delete_mapping(conf['connectors']['elasticsearch']['index'], type_doc)
            time.sleep(1)
        except TransportError as e:
            logger.info("Le type de document %s n'existe pas sur l'index %s", type_doc, conf['connectors']['elasticsearch']['index'])
            pass

        try:
            es.indices.put_mapping(index=conf['connectors']['elasticsearch']['index'], doc_type=type_doc, body=es_mappings['georequetes'][type_doc])
            time.sleep(1)
        except KeyError as e:
            logger.info("Aucun mapping personnlisé n'a été spécifié pour le type de document %s : mapping auto.", type_doc)
            pass

    # On lit dans un fichier
    if type_doc in ['communes','departements','regions']:
        reader = JsonFileio()
        swal.set_reader(reader, p_file=source_file)
    elif type_doc in ['communes_pj']:
        reader = CSVio()
        swal.set_reader(reader, p_file=source_file, p_delimiter='|')
    elif type_doc in ['requetes']:
        reader = ESio(conf['connectors']['elasticsearch']['host'], 
                  conf['connectors']['elasticsearch']['port'], 
                  conf['connectors']['elasticsearch']['bulk_size'])
        swal.set_reader(reader, p_index='syn_es_data_geo')

    # On écrit dans ElasticSearch
    writer = ESio(conf['connectors']['elasticsearch']['host'],
                  conf['connectors']['elasticsearch']['port'],
                  conf['connectors']['elasticsearch']['bulk_size'])
    swal.set_writer(writer, p_index=conf['connectors']['elasticsearch']['index'], p_timeout=30)

    # On transforme la donnée avec la fonction
    swal.set_process(file_to_elasticsearch, p_type=type_doc, p_es_conn=es, p_es_index=conf['connectors']['elasticsearch']['index'], p_arguments=arguments)

    if arguments['--init']:
        logger.debug("Opération d'initialisation")
    elif arguments['--update']:
        logger.debug("Opération de mise à jour")
    else:
        logger.error("Type d'opération non défini")

    logger.debug("Indexation sur %s du type de document %s", conf['connectors']['elasticsearch']['index'], type_doc)
    
    swal.run(1)

    logger.debug("Opération terminée pour le type de document %s ", type_doc)

if __name__ == '__main__':
    run_import()