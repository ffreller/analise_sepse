from create_excel import create_excel_files, gather_info_for_worksheets
from preprocess import preprocess_base, preprocess_prescricoes, preprocess_evolucao
from dbcomms import retrieve_last_month_data_from_dbtasy
from send_email import send_standard_mail_prod, send_standard_mail_test
from src.helper_functions import delete_month_files, print_with_time
from traceback import format_exc
from argparse import ArgumentParser


def ExecuteProgram(test, download_data=True, preprocess=True, create_files=True, sendEmail=True, delete_files=True):
    print()
    print('*'*80)
    if test:
        print('TESTE!')
    success = True
    if download_data:
        success = retrieve_last_month_data_from_dbtasy()     
    
    if success:
        if preprocess:
            try:
                preprocess_base()
            except Exception:
                print_with_time('Erro ao processar dataset base')
                print(format_exc())
                return
            
            try:
                preprocess_prescricoes()
            except:
                print_with_time('Erro ao processar prescrições')
                print(format_exc())
                return
            
            try:
                preprocess_evolucao(enfermagem=False)
            except:
                print_with_time('Erro ao processar evoluções médicas')
                print(format_exc())
                return
            
            try:
                preprocess_evolucao(enfermagem=True)
            except:
                print_with_time('Erro ao processar evoluções da enfermagem')
                print(format_exc())
                return
        
        if create_files:
            try:
                df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_, hemocultura_, antibiotico_  = gather_info_for_worksheets()
            except:
                print_with_time('Erro ao coletar informações para as planilhas')
                print(format_exc())
                return

            try:
                create_excel_files(df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_,  hemocultura_, antibiotico_)
            except:
                print_with_time('Erro ao criar arquivos de excel')
                print(format_exc())
                return
        
        if sendEmail:
            try:
                if test:
                    send_standard_mail_test()
                else:
                    send_standard_mail_prod()
            except:
                print_with_time('Erro ao enviar emails')
                print(format_exc())
                return

        if delete_files:
            try:
                delete_month_files()
            except:
                print_with_time('Erro ao deletar arquivos do mês')
                print(format_exc())
                return

    
if __name__ == '__main__':
    parser = ArgumentParser(description="My parser")
    parser.add_argument('--teste', dest='test', action='store_true')
    parser.set_defaults(test=False)
    args = parser.parse_args()
    test = args.test
    ExecuteProgram(test=test)
    # ExecuteProgram(download_data=False, preprocess=False, create_files=True,
    #                sendEmail=False, test=True, delete_files=False)
    