import logging
from logging.handlers import RotatingFileHandler

# Logger de l'application
logger = logging.getLogger('editofiltres')
logger.compteur = 0

logger_batch_api = logging.getLogger('editofiltres_batch_api')

# Configuration du logger
def configure(p_level,p_dir=None,p_filename=None,p_max_filesize=100000,p_max_files=1,p_prefixe=None):
    #logger = logging.getLogger('editofiltres')
    # default value
    logger.setLevel(logging.DEBUG)

    # Loggers des librairies tierces
    les = logging.getLogger('elasticsearch')
    lsw = logging.getLogger('swallow')

    # default value
    les.setLevel(logging.ERROR)
    lsw.setLevel(logging.WARNING)

    # Format identique pour tous les handlers
    if p_prefixe:
        formatter = logging.Formatter('['+p_prefixe+'] %(asctime)s :: %(levelname)s :: %(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # Handler console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(p_level)
    stream_handler.setFormatter(formatter)
    lsw.addHandler(stream_handler)
    logger.addHandler(stream_handler)

    if p_dir is not None:
        # Handler de type Fichier
        file_path = p_dir + '/' + p_filename
        file_handler = RotatingFileHandler(file_path, 'a', p_max_filesize, p_max_files)
        file_handler.setLevel(p_level)
        file_handler.setFormatter(formatter)
        les.addHandler(file_handler)
        lsw.addHandler(file_handler)
        logger.addHandler(file_handler)

# Configuration du logger
def configure_batch_api_logger(p_level,p_dir=None,p_filename=None,p_max_filesize=100000,p_max_files=1,p_prefixe=None):
    #logger = logging.getLogger('editofiltres')
    # default value
    logger_batch_api.setLevel(logging.DEBUG)

    # Format identique pour tous les handlers
    if p_prefixe:
        formatter = logging.Formatter('['+p_prefixe+'] %(asctime)s :: %(levelname)s :: %(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # Handler console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(p_level)
    stream_handler.setFormatter(formatter)
    logger_batch_api.addHandler(stream_handler)

    if p_dir is not None:
        # Handler de type Fichier
        file_path = p_dir + '/' + p_filename
        file_handler = RotatingFileHandler(file_path, 'a', p_max_filesize, p_max_files)
        file_handler.setLevel(p_level)
        file_handler.setFormatter(formatter)
        logger_batch_api.addHandler(file_handler)