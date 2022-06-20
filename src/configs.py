from credentials import SMTP_SERVER


LOGGING_CONFIG = { 
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': { 
        'standard': { 
            'format':'%(asctime)s - %(levelname)s: %(message)s',
            'datefmt':'%d/%m/%Y %H:%M:%S'
        },
    },
    'handlers': { 
        'stream': { 
            'level': 'NOTSET',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'file': { 
            'level': 'NOTSET',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'log.log',
            'mode': 'a'
        },
        'file_error': { 
            'level': 'ERROR',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'errors.log',
            'mode': 'a'
        },
        'email':{
            'class': 'logging.handlers.SMTPHandler',
            'formatter': 'standard',
            'mailhost': SMTP_SERVER,
            'fromaddr': "relatorios.tasy@haoc.com.br",
            'toaddrs': ["datalab@haoc.com.br"],
            'subject': '[SEPSE] Erro na execução do script de coleta de pacientes Sepse'
        }

    },
    'loggers': { 
        '': {  # root logger
            'handlers': ['stream'],
            'level': 'NOTSET',
            'propagate': False
        },
        'standard': { 
            'handlers': ['stream', 'file'],
            'level': 'NOTSET',
            'propagate': False
        },
        'error': { 
            'handlers': ['file_error', 'email'],
            'level': 'ERROR',
            'propagate': False
        }
    } 
}