def ExecuteProgram(prod, download_data=True, preprocess=True, create_files=True, send_mail=True):
    from create_excel import create_excel_files, gather_info_for_worksheets
    from preprocess import preprocess_base, preprocess_prescricoes, preprocess_evolucao
    from dbcomms import retrieve_last_month_data_from_dbtasy
    from send_email import send_standard_mail
    from src.helper_functions import delete_month_files
    from traceback import format_exc
    from logging import getLogger
    
    if prod:
        download_data = True
        preprocess = True
        create_files=True
        send_mail = True  
    
    logger = getLogger('standard')
    error_logger_name = 'error_prod' if prod else 'error_test'
    error_logger = getLogger(error_logger_name)
    
    success = True        
    if download_data:
        try:
            success = retrieve_last_month_data_from_dbtasy()
            # success = retrieve_specific_dates_from_dbtasy('14/06/2021', '14/01/2022') 
        except:
            logger.error('Erro na comunicação com o DB: %s' % format_exc())
            error_logger.error('Erro na comunicação com o DB: %s' % format_exc())
            return       
    
    if success:
        if preprocess:
            try:
                preprocess_base()
            except Exception:
                logger.error('Erro ao processar dataset base: %s' % format_exc())
                error_logger.error('Erro ao processar dataset base: %s' % format_exc())
                return
            
            try:
                preprocess_prescricoes()
            except Exception:
                logger.error('Erro ao processar prescrições: %s' % format_exc())
                error_logger.error('Erro ao processar prescrições: %s' % format_exc())
                return
            
            try:
                preprocess_evolucao(enfermagem=False)
            except Exception:
                logger.error('Erro ao processar evoluções médicas: %s' % format_exc())
                error_logger.error('Erro ao processar evoluções médicas: %s' % format_exc())
                return
            
            try:
                preprocess_evolucao(enfermagem=True)
            except Exception:
                logger.error('Erro ao processar evoluções da enfermagem: %s' % format_exc())
                error_logger.error('Erro ao processar evoluções da enfermagem: %s' % format_exc())
                return
        
        if create_files:
            try:
                df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_, hemocultura_, antibiotico_  = gather_info_for_worksheets()
            except Exception:
                logger.error('Erro ao coletar informações para as planilhas: %s' % format_exc())
                error_logger.error('Erro ao coletar informações para as planilhas: %s' % format_exc())
                return

            try:
                create_excel_files(df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_,  hemocultura_, antibiotico_)
            except Exception:
                logger.error('Erro ao criar arquivos de excel: %s' % format_exc())
                error_logger.error('Erro ao criar arquivos de excel: %s' % format_exc())
                return
        
        if send_mail:
            try:
                send_standard_mail(prod=prod)
            except Exception:
                logger.error('Erro ao enviar emails: %s' % format_exc())
                error_logger.error('Erro ao enviar emails: %s' % format_exc())
                return

            try:
                delete_month_files()
            except Exception:
                logger.error('Erro ao deletar arquivos do mês: %s' % format_exc())
                error_logger.error('Erro ao deletar arquivos do mês: %s' % format_exc())
                return
        logger.debug('FIM: Sucesso ao executar script\n')

    
if __name__ == '__main__':
    from argparse import ArgumentParser
    import logging.config
    from configs import LOGGING_CONFIG
    
    logging.config.dictConfig(LOGGING_CONFIG)
    
    parser = ArgumentParser(description="My parser")
    parser.add_argument('--prod', dest='prod', action='store_true')
    parser.add_argument('--no-email', dest='no_email', action='store_true')
    parser.add_argument('--no-download', dest='no_download', action='store_true')
    parser.add_argument('--no-preprocess', dest='no_preprocess', action='store_true')
    parser.set_defaults(prod=False)
    parser.set_defaults(no_email=False)
    parser.set_defaults(no_download=False)
    parser.set_defaults(no_preprocess=False)
    
    args = parser.parse_args()
    send_mail = not args.no_email
    download_data = not args.no_download
    preprocess = not args.no_preprocess
    prod = args.prod      
    
    ExecuteProgram(prod=prod, send_mail=send_mail, download_data=download_data, preprocess=preprocess)