def campo_sepse_med(text):
    if type(text) == str:
        if "Sepse :" in text:
            trecho = text[text.find("Sepse :")+len("Sepse :")+2: text.find("Sepse :")+12]
            if trecho.lower() == 'sim':
                return True
    return False


def remove_campo_sepse_from_text_med(text):
    if type(text) == str:
        if "Sepse :" in text:
            new = text.replace('Sepse :', '')
            return new
    return text


def remove_antecedentes_from_text(text):
    import re
    pattern_antecedentes = re.compile('Antecedentes ?:')
    if type(text) == str:
        match = pattern_antecedentes.search(text)
        if match:
            before_antecedentes = text[:match.start()]
            if 'Tabagismo' in text:
                after_tabagismo = text[text.find('Tabagismo'):]
                new = before_antecedentes + after_tabagismo
                return new
            return text
    return text


def text_contains_sepse_expression(text):
    import re
    key_words_pattern_sepse = re.compile('choque *(s(e|é)ptico|refrat(a|á)rio|misto)|seps(e|is)|septicemia', re.IGNORECASE)
    key_words_pattern_sepse_meio = re.compile('(choque *(s(e|é)ptico|refrat(a|á)rio|misto) *\?|seps(e|is) *\?|septicemia *\?)|(suspeita *de *choque *(s(e|é)ptico|refrat(a|á)rio|misto)|suspeita *de *seps(e|is)|suspeita *de *septicemia)', re.IGNORECASE)
    if type(text) == str:
        if 'paciente comparece para biopsia de prostata' not in text.lower().replace('ó', 'o'):
            match = key_words_pattern_sepse.search(text)
            if match:
                match_meio = key_words_pattern_sepse_meio.search(text)
                if match_meio:
                    return 0.5, match_meio.group(0)
                return 1, match.group(0)         
    return 0, 'NÃO'


def text_contains_cuidados_paliativos(text):
    import re
    key_words_pattern_paliativo = re.compile('cuidados? paliativos?', re.IGNORECASE)
    key_words_patter_paliativo_ignorar = re.compile('cuidados paliativos *: * (sem limitação de suporte orgânico invasivo)|não', re.IGNORECASE)
    if type(text) == str:
        match = key_words_pattern_paliativo.search(text)
        if match:
            match_ignorar = key_words_patter_paliativo_ignorar.search(text)
            if not match_ignorar:
                return True       
    return False


def text_contains_codigo_amarelo(text):
    if type(text) == str:
        if 'código amarelo/avaliação clínica' in text[:50].lower():
            return True       
    return False


def get_time_between_evolucao_med_com_sepse(df__):
    from datetime import timedelta
    df_ = df__.copy()
    df_['tempo_entre_evolucao_med_com_sepse'] = df_['DT_EVOLUCAO_MED'].diff()
    df_.loc[df_['NR_ATENDIMENTO'].diff() != 0, 'tempo_entre_evolucao_med_com_sepse'] = float('nan')
    df_['tempo_entre_evolucao_med_com_sepse>72'] = df_['tempo_entre_evolucao_med_com_sepse'] > timedelta(days=3)
    return df_[['tempo_entre_evolucao_med_com_sepse', 'tempo_entre_evolucao_med_com_sepse>72']].copy()


def my_rtf_to_text(rtf):
    from striprtf.striprtf import rtf_to_text
    return rtf_to_text(rtf, errors='ignore')


def print_with_time(txt):
    from datetime import datetime
    agora = datetime.now()
    print(f"{agora.strftime('%d/%m/%Y %H:%M:%S')} - {txt}")
    
    
def apply_rtf_and_bold_expression(text, all_expressions):
    new_text = """{\\rtf1 {\\colortbl;\\red0\\green0\\blue0;\\red255\\green0\\blue0;}""" + text + "}"
    for expression in all_expressions:
        if expression == 'NÃO' or '?' in expression or 'suspeita' in expression:
            continue
        if expression in new_text:
            new_text = new_text.replace(expression, f"\\b {expression} \\b0")
        elif expression.capitalize() in new_text:
            new_text = new_text.replace(expression.capitalize(), f"\\b {expression.capitalize()} \\b0")
        else:
            new_text = new_text.replace(expression.title(), f"\\b {expression.title()} \\b0")
        
        new_text = new_text.replace('\\b ', '\\cf2\\b ')
        new_text = new_text.replace(' \\b0', ' \\b0\\cf')
    return new_text 


def hemocultura_antibiotico_dentro_do_periodo(row, horarios_sepse_dict):
    n_atend = row['NR_ATENDIMENTO']
    data = row['DT_LIBERACAO']
    data_sepse = horarios_sepse_dict[n_atend]
    diff_horas = (data - data_sepse).total_seconds()/3600
    return round(diff_horas, 2)


def delete_month_files():
    from os import remove as os_remove
    selecionados_fn_paulista, selecionados_fn_vergueiro = get_selecionados_fn_for_month()
    os_remove(selecionados_fn_paulista)
    os_remove(selecionados_fn_vergueiro)
    
    
def get_last_month_and_year():
    from datetime import datetime
    today = datetime.today()
    target_month, target_year = today.month - 1, today.year
    if target_month == 0:
        target_month = 12
        target_year -= 1
    return target_month, target_year, today.month, today.year


def get_selecionados_fn_for_month(unidade='ambas'):
    from src.definitions import PROCESSED_DATA_DIR
    from datetime import datetime
    assert unidade.lower() in ['ambas', 'paulista', 'vergueiro'], print("Unidade precisa ser 'ambas', 'paulista' ou 'vergueiro'")
    target_month, year_of_target_month, _, _ = get_last_month_and_year()
    target_date = datetime(year=year_of_target_month, month=target_month, day=1)
    if unidade.lower() == 'ambas' or unidade.lower() == 'paulista':
        selecionados_fn_paulista = PROCESSED_DATA_DIR / f"Selecionados_Paulista_{target_date.strftime('%m-%Y')}.xlsx"
        if unidade.lower() == 'paulista':
            return selecionados_fn_paulista
    if unidade.lower() == 'ambas' or unidade.lower() == 'vergueiro':
        selecionados_fn_vergueiro = PROCESSED_DATA_DIR / f"Selecionados_Vergueiro_{target_date.strftime('%m-%Y')}.xlsx"
        if unidade.lower() == 'vergueiro':
            return selecionados_fn_vergueiro
    return selecionados_fn_paulista, selecionados_fn_vergueiro


def dfs_generator(*args):
    for arg in args:
        yield arg.copy()

  
def sheet_names_generator(*args):
    for arg in args:
        yield arg
    
