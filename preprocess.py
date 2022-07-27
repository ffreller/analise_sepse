def preprocess_base():
    from pandas import read_pickle, to_datetime
    from src.definitions import RAW_DATA_DIR, INTERIM_DATA_DIR
    from logging import getLogger

    logger = getLogger('standard')
    base_useful_cols = ['NR_ATENDIMENTO', 'NR_PRONTUARIO', 'NM_PESSOA_FISICA', 'DT_NASCIMENTO','DT_ENTRADA',
                    'DT_ALTA', 'IE_SEXO', 'ID_MEDICO', 'DS_CLINICA', 'CD_DOENCA', 'DS_MOTIVO_ALTA', 'DT_ENTRADA_GRUPO',
                    'DT_SAIDA_GRUPO','DT_CODIGO_AMARELO', 'CD_ESTABELECIMENTO']
    cid_sepses = 'A41'
    
    #Lendo o dataset
    df0 = read_pickle(RAW_DATA_DIR/'Dados_Básicos.pickle')
    #Filtrando colunas que serão usadas
    df0 = df0.loc[:, base_useful_cols]
    # Criando coluna referente à presença de CID de sepse
    df0['cid_sepse'] = df0['CD_DOENCA'].astype(str).str.contains(cid_sepses)
    
    for col in ['DT_ENTRADA_GRUPO', 'DT_SAIDA_GRUPO']:
        cond1  = df0[col] < df0['DT_ENTRADA']
        cond2 = df0[col] > df0['DT_ALTA']
        df0.loc[cond1 | cond2, col] = to_datetime('NaT')
        
    # Criando coluna que indica acionamento do código amarelo
    df0['CODIGO_AMARELO'] = df0['DT_CODIGO_AMARELO'].isna() == False
    # Criando coluna que indicado entra no grupo sepse
    df0['ENTRADA_GRUPO'] = df0['DT_ENTRADA_GRUPO'].isna() == False

    assert df0['NR_ATENDIMENTO'].is_unique, "Há números de atendimento duplicados"
    
    # Salvar dataset criado
    df0.to_pickle(INTERIM_DATA_DIR/'base.pickle')
    logger.debug('Sucesso ao processar dataset base')


def preprocess_prescricoes():
    from src.definitions import RAW_DATA_DIR, INTERIM_DATA_DIR
    from pandas import read_pickle
    from logging import getLogger

    logger = getLogger('standard')
    colunas_prescricao = ['NR_ATENDIMENTO', 'DT_ITEM_PRESCRITO', 'DT_ITEM_EXECUTADO', 'Noradrenalina', 'PROTOCOLO_SEPSE', 'NR_PRESCRICAO']
    codigos_noradrenalina = [481471, 259826, 429303, 1434, 406272]

    #Lendo o dataset
    df0 = read_pickle(RAW_DATA_DIR/'Prescrição_Medicamentos.pickle')
    df0['Noradrenalina'] = df0['CD_MATERIAL'].isin(codigos_noradrenalina)
    df0.loc[~df0['DS_TIPO_PROTOCOLO'].isna(), 'PROTOCOLO_SEPSE'] = True
    df0['PROTOCOLO_SEPSE'] = df0['PROTOCOLO_SEPSE'].fillna(False)
    df0 = df0.loc[:, colunas_prescricao].copy()
    # Salvar dataset criado
    df0.to_pickle(INTERIM_DATA_DIR/'prescricoes.pickle')
    logger.debug('Sucesso ao processar prescrições')
       

def preprocess_evolucao(enfermagem):
    from src.definitions import RAW_DATA_DIR, INTERIM_DATA_DIR
    from pandas import read_pickle
    from logging import getLogger
    from src.helper_functions import my_rtf_to_text
    from src.HTMLStripper import strip_html_tags

    logger = getLogger('standard')

    pickle_fn = 'Evolução_Enfermagem.pickle' if enfermagem else 'Evolução_Médica.pickle'
    #Lendo o dataset
    df0 = read_pickle(RAW_DATA_DIR/pickle_fn)
    df0['EVOLUCAO'] = df0['DS_EVOLUCAO'].apply(my_rtf_to_text)
    assert df0['EVOLUCAO'].isna().sum() == 0,\
        f"Valores nan na coluna da evolucao {'enfermagem' if enfermagem else 'médica'}"
    df0['EVOLUCAO'] = df0['EVOLUCAO'].apply(strip_html_tags)
    df0['EVOLUCAO'] = df0['EVOLUCAO'].str.replace(u'\xa0', u' ')
    
    # Mudando nome das colunas nos datasets de evolução para diferenciá-los futuramente
    suffix = '_ENF' if enfermagem else '_MED'
    df0.columns = [col+suffix if col != 'NR_ATENDIMENTO' else col for col in df0.columns]
    
    # Filtrando dados de evolução da enfermagem e deixando apenas os que têm template sepse indicado
    if enfermagem:
        from src.helper_functions import extract_horario_from_text, datetime_from_horario_protocolo
        df0 = df0[df0['EVOLUCAO_ENF'].str.lower().str.contains('confirmação de protocolo sepse')].copy()
        df0['EVOLUCAO_ENF'] = df0['EVOLUCAO_ENF'].str.replace('\u200b', '')
        regex_horario_hmc = "Horário da Coleta da Hemocultura: *([^A-Z]+)"
        regex_horario_adm_atb = "Horário da administração do antibiótico: *([^A-Z]+)(?=antibiótico)"
        regex_horario_aval_medica = "(\d?\d_*:? *_*\d\d)_* *Horário da avaliação médica"

        df0['Horário da coleta da hemocultura (extração automática de texto)'] = \
            df0['EVOLUCAO_ENF'].apply(lambda txt: extract_horario_from_text(regex_horario_hmc, txt))
        df0['Horário da administração do antibiótico (extração automática de texto)'] =\
            df0['EVOLUCAO_ENF'].apply(lambda txt: extract_horario_from_text(regex_horario_adm_atb, txt))
        df0['Horário da avaliação médica (extração automática de texto)'] =\
            df0['EVOLUCAO_ENF'].apply(lambda txt: extract_horario_from_text(regex_horario_aval_medica, txt))

        for new_col in ['Data e hora da coleta da hemocultura (Anotação padrão do Protocolo)', 'Data e hora da administração do antibiótico (Anotação padrão do Protocolo)',
                    'Data e hora da avaliação médica (Anotação padrão do Protocolo)']:
            old_col = new_col.replace("Data e hora", "Horário").replace("Anotação padrão do Protocolo", "extração automática de texto")
            df0.loc[df0[old_col] != "NO MATCH", new_col] = datetime_from_horario_protocolo(df0.loc[df0[old_col] != "NO MATCH"], old_col)
            
        df0['TEMPLATE_ENF'] = 1
    else:
        from src.helper_functions import text_contains_expression, campo_sepse_med, remove_campo_sepse_from_text_med,\
            remove_antecedentes_from_text, text_contains_codigo_amarelo, text_contains_cuidados_paliativos, get_regex_suspeita_interrogacao
        # Criando coluna que indica se atendimento tinha 'Sim' assinalado no campo sepse da evolução médica
        df0['campo_sepse_evolucao_med'] = df0['EVOLUCAO_MED'].apply(campo_sepse_med)
        # Criando coluna que indica se evolução médica do paciente faz referência a cuidados paliativos
        df0['cuidado_paliativo'] = df0['EVOLUCAO_MED'].apply(text_contains_cuidados_paliativos)
        # Criando coluna que indica se evolução médica do paciente faz referência a acionamento de código amarelo
        df0['CODIGO_AMARELO_EVOL'] = df0['EVOLUCAO_MED'].apply(text_contains_codigo_amarelo)
        # Remove antecedentes da evolução médica
        df0['EVOLUCAO_MED'] = df0['EVOLUCAO_MED'].apply(remove_antecedentes_from_text)
        # Removendo campo sepse da evolução médica (para avaliar a presença de expressões sem levá-lo em consideração)
        df0['EVOLUCAO_MED2'] = df0['EVOLUCAO_MED'].apply(remove_campo_sepse_from_text_med)
        df0['EVOLUCAO_MED2'] = df0['EVOLUCAO_MED2'].str.replace('oriento antes sobre as possíveis complicações ITU, sepse', '')
        # Criando coluna que indica se evolução médica do paciente faz referência a alguma expressão relacionada a sepse e 
        # coluna para indicar qual foi a expressão sepse encontrada, se houve alguma
        regex_string = '(choque *(s(e|é)ptico|refrat(a|á)rio|misto))|(seps(e|is))|(septicemia)'
        regex_half_string = get_regex_suspeita_interrogacao(regex_string)
        df0[['sepse_expression_evolucao_med', 'match']] = df0.apply(
            lambda x: text_contains_expression(
                text=x['EVOLUCAO_MED2'], regex_string=regex_string, regex_half_string=regex_half_string
                ), axis=1, result_type="expand"
            )
        logger.debug("Soma da coluna 'sepse_expression_evolucao_med': %s" % df0['sepse_expression_evolucao_med'].sum())
        df0.drop(['DS_EVOLUCAO_MED', 'EVOLUCAO_MED2'], axis=1, inplace=True)
        # Ordenando por número de atendimento e data de evolução
        df0.sort_values(['NR_ATENDIMENTO','DT_EVOLUCAO_MED'], inplace=True)
        
        
    # Salvar dataset criado
    df0.to_pickle(INTERIM_DATA_DIR/pickle_fn)
    str_tipo = 'da enfermagem' if enfermagem else 'médicas'
    logger.debug("Sucesso ao processar evoluções %s" % str_tipo)

    
if __name__ == '__main__':
    # preprocess_base()
    # preprocess_prescricoes()
    # preprocess_evolucao(enfermagem=False)
    preprocess_evolucao(enfermagem=True)