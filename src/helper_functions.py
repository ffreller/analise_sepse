from lib2to3.pgen2.token import MINUS


def campo_sepse_med(text):
    if isinstance(text, str):
        if "Sepse :" in text:
            trecho = text[text.find("Sepse :")+len("Sepse :")+2: text.find("Sepse :")+12]
            if trecho.lower() == 'sim':
                return True
    return False


def remove_campo_sepse_from_text_med(text):
    if isinstance(text, str):
        if "Sepse :" in text:
            new = text.replace('Sepse :', '')
            return new
    return text


def remove_antecedentes_from_text(text):
    import re
    pattern_antecedentes = re.compile('Antecedentes ?:')
    if isinstance(text, str):
        match = pattern_antecedentes.search(text)
        if match:
            before_antecedentes = text[:match.start()]
            if 'Tabagismo' in text:
                after_tabagismo = text[text.find('Tabagismo'):]
                new = before_antecedentes + after_tabagismo
                return new
            return text
    return text


def text_contains_expression(text, regex_string, regex_half_string):
    if isinstance(text, str):
        from re import compile, findall, IGNORECASE
        key_words_pattern_sepse = compile(regex_string, IGNORECASE)
        key_words_pattern_sepse_half = compile(regex_half_string, IGNORECASE)
        positive_results = findall(key_words_pattern_sepse, text)
        if positive_results:
            half_results = findall(key_words_pattern_sepse_half, text)
            if len(half_results) >= len(positive_results):
                from logging import getLogger
                logger = getLogger('standard')
                if isinstance(half_results[0], str):
                    logger.warning('Regex (half) com lista de strings: %s' % half_results)
                if len(half_results) > len(positive_results):
                    logger.warning('PROBLEMA REGEX: Texto:%s POsitive:%s\n Half:%s\n' % (text, positive_results, half_results))
                half_results = [subelement for element in half_results for subelement in element]
                return .5, max(half_results, key=len)
            
            if isinstance(positive_results[0], str):
                from logging import getLogger
                logger = getLogger('standard')
                logger.warning('Regex (positive) com lista de strings: %s' % positive_results)
            positive_results = [subelement for element in positive_results for subelement in element]   
            return 1, max(positive_results, key=len) 
    return 0, 'NÃO'


def get_regex_suspeita_interrogacao(text):
    expressions = text.split(')|(')
    expressions[0] = expressions[0][1:]
    expressions[-1] = expressions[-1][:-1]
    results = []
    for expression in expressions:
        results.append(f'({expression} *\?)')
        results.append(f'(suspeita *de *{expression})')
    return f"({'|'.join(results)})"


def text_contains_cuidados_paliativos(text):
    if isinstance(text, str):
        import re
        key_words_pattern_paliativo = re.compile('cuidados? paliativos?', re.IGNORECASE)
        key_words_patter_paliativo_ignorar = re.compile('cuidados paliativos *: * (sem limitação de suporte orgânico invasivo)|não', re.IGNORECASE)
        match = key_words_pattern_paliativo.search(text)
        if match:
            match_ignorar = key_words_patter_paliativo_ignorar.search(text)
            if not match_ignorar:
                return True       
    return False


def text_contains_codigo_amarelo(text):
    if isinstance(text, str):
        if 'código amarelo/avaliação clínica' in text[:50].lower():
            return True       
    return False


def get_time_between_evolucao_med_com_sepse(df__):
    from datetime import timedelta
    df_ = df__.copy()
    df_['tempo_entre_evolucao_med_com_sepse'] = df_['DT_EVOLUCAO_MED'].diff().dt.total_seconds()/3600
    df_.loc[df_['NR_ATENDIMENTO'].diff() != 0, 'tempo_entre_evolucao_med_com_sepse'] = float('nan')
    df_['tempo_entre_evolucao_med_com_sepse>72'] = df_['tempo_entre_evolucao_med_com_sepse'] > 72
    return df_[['tempo_entre_evolucao_med_com_sepse', 'tempo_entre_evolucao_med_com_sepse>72']].copy()


def my_rtf_to_text(rtf):
    from striprtf.striprtf import rtf_to_text
    return rtf_to_text(rtf, errors='ignore')
    
    
def apply_rtf_and_bold_expression(text, all_expressions):
    if not isinstance(text, str):
        return text
    expression_found = False
    new_text = """{\\rtf1 {\\colortbl;\\red0\\green0\\blue0;\\red255\\green0\\blue0;}""" + text + "}"
    for expression in all_expressions:
        if (expression == 'NÃO') or ('?' in expression) or ('suspeita' in expression):
            continue
        if expression in new_text:
            new_text = new_text.replace(expression, f"\\cf2\\b {expression} \\b0\\cf'")
            expression_found = True
    return new_text if expression_found else text


def hemocultura_antibiotico_dentro_do_periodo(row, horarios_sepse_dict):
    n_atend = row['NR_ATENDIMENTO']
    data = row['DT_PRESCRICAO']
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
    assert unidade.lower() in ['ambas', 'paulista', 'vergueiro'], "Unidade precisa ser 'ambas', 'paulista' ou 'vergueiro'"
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
    

def format_hours_deltatime(number):
    from math import isnan
    if isnan(number):
        return ''
    final_str = '-' if number < 0 else ''
    number = abs(number)
    decimal_hours = number - int(number)
    minutes = decimal_hours * 60
    decimal_minutes = minutes - int(minutes)
    seconds = decimal_minutes * 60
    
    return final_str+'{:02}:{:02}:{:02}'.format(int(number), int(minutes), int(seconds))

def extract_horario_from_text(regex_string, text):
    if not isinstance(text, str):
        return ""
    from re import compile, findall
    pattern = compile(regex_string)
    matches  = findall(pattern, text)
    if not matches:
        return "NO MATCH"
    match = matches[0].replace("_", "")
    return match


def datetime_from_horario_protocolo(df, coluna_horario):
    from pandas import Timedelta, to_datetime
    df_ = df.copy()
    df_['horario_liberacao'] = df_['DT_LIBERACAO_ENF'].dt.hour.astype(float) + df_['DT_LIBERACAO_ENF'].dt.minute.astype(int)/10
    df_['horas_texto'] = df_[coluna_horario].apply(lambda x: x[:2] if x[2]==':' else x[:1]).astype(int)
    df_['minutos_texto'] = df_[coluna_horario].apply(lambda x: x[3:5] if x[2]==':' else x[2:4]).astype(int)
    df_['horario_texto'] = df_['horas_texto'] + df_['minutos_texto']/10
    df_.loc[df_['horario_texto'] < df_['horario_texto'], 'dia_real'] = to_datetime((df_['DT_LIBERACAO_ENF'] - Timedelta(days=1)).dt.date)
    df_.loc[df_['horario_texto'] >= df_['horario_texto'], 'dia_real'] = to_datetime(df_['DT_LIBERACAO_ENF'].dt.date)
    
    df_['data_hora_real'] = df_[['dia_real', 'horas_texto', 'minutos_texto']].apply(
        lambda x: x[0] + Timedelta(hours=x[1]) + Timedelta(minutes=x[2]),
        axis=1
        )

    return df_['data_hora_real'].copy()