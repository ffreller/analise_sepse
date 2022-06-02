from create_excel import create_excel_files, gather_info_for_worksheets
from preprocess import preprocess_base, preprocess_prescricoes, preprocess_evolucao
from dbcomms import retrieve_last_month_data_from_dbprod
from send_email import send_standard_mail_prod, send_standard_mail_test
from src.helper_functions import delete_month_files, print_with_time
import traceback
   

def ExecuteProgram(download_data=True, preprocess=True, create_files=True, sendEmail=True, prod=True, delete_files=True):
    success = True
    if download_data:
        success = retrieve_last_month_data_from_dbprod()     
    
    if success:
        if preprocess:
            try:
                preprocess_base()
            except Exception:
                print_with_time('Erro ao processar dataset base')
                print(traceback.format_exc())
                return
            
            try:
                preprocess_prescricoes()
            except:
                print_with_time('Erro ao processar prescrições')
                print(traceback.format_exc())
                return
            
            try:
                preprocess_evolucao(enfermagem=False)
            except:
                print_with_time('Erro ao processar evoluções médicas')
                print(traceback.format_exc())
                return
            
            try:
                preprocess_evolucao(enfermagem=True)
            except:
                print_with_time('Erro ao processar evoluções da enfermagem')
                print(traceback.format_exc())
                return
        
        if create_files:
            try:
                df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_, hemocultura_, antibiotico_  = gather_info_for_worksheets()
            except:
                print_with_time('Erro ao coletar informações para as planilhas')
                print(traceback.format_exc())
                return

            try:
                create_excel_files(df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_,  hemocultura_, antibiotico_)
            except:
                print_with_time('Erro ao criar arquivos de excel')
                print(traceback.format_exc())
                return
        
        if sendEmail:
            try:
                if prod:
                    send_standard_mail_prod()
                else:
                    send_standard_mail_test()
            except:
                print_with_time('Erro ao enviar emails')
                print(traceback.format_exc())
                return

        if delete_files:
            try:
                delete_month_files()
            except:
                print_with_time('Erro ao deletar arquivos do mês')
                print(traceback.format_exc())
                return

    
if __name__ == '__main__':
    ExecuteProgram()
    # ExecuteProgram(prod=False)
    # ExecuteProgram(download_data=False, preprocess=False, create_files=True,
    #                sendEmail=True, prod=False, delete_files=True)
    