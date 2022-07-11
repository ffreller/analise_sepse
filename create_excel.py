def gather_info_for_worksheets():
    
    from src.helper_functions import get_time_between_evolucao_med_com_sepse, hemocultura_antibiotico_dentro_do_periodo, format_hours_deltatime
    from src.definitions import INTERIM_DATA_DIR, RAW_DATA_DIR
    from pandas import read_pickle, to_datetime
    from logging import getLogger
    
    logger = getLogger('standard')

    atends_coletados = set()
    #Lendo os datasets
    base = read_pickle(INTERIM_DATA_DIR/'base.pickle')
    evol_med = read_pickle(INTERIM_DATA_DIR/'Evolução_Médica.pickle')
    evol_enf = read_pickle(INTERIM_DATA_DIR/'Evolução_Enfermagem.pickle')
    prescricoes = read_pickle(INTERIM_DATA_DIR/'prescricoes.pickle')
    movimentacoes = read_pickle(RAW_DATA_DIR/'Movimentações_Setores.pickle')
    hemocultura = read_pickle(RAW_DATA_DIR/'Hemocultura.pickle')
    antibiotico = read_pickle(RAW_DATA_DIR/'Antibiótico.pickle')
    
    movimentacoes.columns = [col.upper() for col in movimentacoes.columns]
    
    for col in ['cid_sepse', 'ENTRADA_GRUPO', 'CODIGO_AMARELO']:
        n_atends_ = base[base[col] == True]['NR_ATENDIMENTO'].unique()
        atends_coletados.update(n_atends_)
    for col in ['sepse_expression_evolucao_med', 'campo_sepse_evolucao_med', 'CODIGO_AMARELO_EVOL']:
        n_atends_ = evol_med[evol_med[col] > 0]['NR_ATENDIMENTO'].unique()
        atends_coletados.update(n_atends_)
    
    atends_coletados.update(evol_enf['NR_ATENDIMENTO'].unique())
    # Criando dataset agrupado da evolução da enfermagem para adicioanr ao resultado final
    enf_counts = evol_enf.groupby('NR_ATENDIMENTO').agg({
        'TEMPLATE_ENF':'sum',
        'DT_EVOLUCAO_ENF':'min'
    })
    
    # Criando dataset agrupado das prescrições para adicioanr ao resultado final
    prescricoes_counts = prescricoes.groupby('NR_ATENDIMENTO').agg({
        'Noradrenalina':'sum',
        'PROTOCOLO_SEPSE':'sum',
        'DT_ITEM_PRESCRITO':'min'
    }).reset_index()
    for col in ['Noradrenalina', 'PROTOCOLO_SEPSE']:
        n_atends_ = prescricoes_counts[prescricoes_counts[col] != 0]['NR_ATENDIMENTO'].unique()
        atends_coletados.update(n_atends_)
    prescricoes_counts.loc[prescricoes_counts['Noradrenalina'] == 0, 'DT_ITEM_PRESCRITO'] = to_datetime('NaT')
    
    # Criando dataset agrupado das evoluções médicas para adicioanr ao resultado final
    evol_med_counts = evol_med.groupby('NR_ATENDIMENTO').agg({
        'campo_sepse_evolucao_med':'sum',
        'CODIGO_AMARELO_EVOL':"sum",
        'sepse_expression_evolucao_med':['sum','mean']
    })
    
    # Selecionando apenas os atendimentos que têm evolução médica com expressão relacionada a sepse
    evol_med_sepse = evol_med.loc[evol_med['sepse_expression_evolucao_med'] > 0].sort_values(['NR_ATENDIMENTO', 'DT_EVOLUCAO_MED']).copy()
    #Coluna com tempo entre evoluções médicas com sepse
    time_between_evolucao_med_com_sepse = get_time_between_evolucao_med_com_sepse(evol_med_sepse)    
    # Criando coluna com menor data de evolução médica que contém expressão relacionada a sepse
    evol_med_sepse_counts = evol_med_sepse.groupby('NR_ATENDIMENTO').agg({'DT_EVOLUCAO_MED':'min'})
    # Trazendo coluna para dataset de evolução médica
    evol_med_counts = evol_med_counts.merge(evol_med_sepse_counts, right_index=True, left_index=True, how='left')
    # Ajustando colunas
    evol_med_counts.columns = ['campo_sepse_evolucao_med_sum', 'CODIGO_AMARELO_EVOL_sum', 'sepse_expression_evolucao_med_sum', 'sepse_expression_evolucao_med_mean', 'primeira_data_com_sepse']
    # Selecionando apenas os atendimentos que têm evolução médica referente a cuidados paliativos	
    evol_med_paliativo = evol_med[evol_med['cuidado_paliativo'] == 1].copy()
    # Criando coluna com menor data de evolução médica que contém expressão relacionada a cuidado paliativo
    evol_med_paliativo_counts = evol_med_paliativo.groupby('NR_ATENDIMENTO').agg({'DT_EVOLUCAO_MED':'min'})
    evol_med_paliativo_counts.columns = ['primeira_data_com_paliativo']
    #Trazendo coluna para dataset de evolução médica
    evol_med_counts = evol_med_counts.merge(evol_med_paliativo_counts, right_index=True, left_index=True, how='left')
    
    #Juntando informações sobre evoluções médicas com base
    df0 = base.merge(evol_med_counts, left_on='NR_ATENDIMENTO', right_index=True, how='left')
    
    #Trazendo informações sobre prescrições para o dataset principal
    df1 = df0.merge(prescricoes_counts, left_on='NR_ATENDIMENTO', right_on='NR_ATENDIMENTO', how='left')
    
    #Trazendo informações sobre voluções da enfermagem para o dataset principal
    df2 = df1.merge(enf_counts, left_on='NR_ATENDIMENTO', right_index=True, how='left')
    df2['campo_sepse_evolucao_med_sum'] = df2['campo_sepse_evolucao_med_sum'].fillna(False)
    df_main = df2[df2['NR_ATENDIMENTO'].isin(atends_coletados)].copy()
    horarios_sepse_dict = df_main[['NR_ATENDIMENTO', 'primeira_data_com_sepse']].groupby('NR_ATENDIMENTO').min().to_dict()['primeira_data_com_sepse']
    
    #Fromatação final
    df_main['CD_ESTABELECIMENTO'] = df0['CD_ESTABELECIMENTO'].map({1:'Paulista', 17:'Vergueiro'})
    for col in ['campo_sepse_evolucao_med_sum', 'CODIGO_AMARELO_EVOL_sum', 'PROTOCOLO_SEPSE', 'Noradrenalina', 'TEMPLATE_ENF']:
        df_main[col] = df_main[col].fillna(0).astype(int)
    df_main = df_main.sort_values(['DT_CODIGO_AMARELO', 'DT_ENTRADA_GRUPO']).drop_duplicates(subset='NR_ATENDIMENTO', keep='first')
    df_main['sepse_expression_evolucao_med_sum'] = df_main['sepse_expression_evolucao_med_sum'] + df_main['campo_sepse_evolucao_med_sum']
    
    cols_names = {
        'NR_ATENDIMENTO':'Número de Atendimento',
        'CD_ESTABELECIMENTO':'Unidade',
        'NR_PRONTUARIO':'Prontuário',
        'NM_PESSOA_FISICA':'Nome do paciente',
        'DT_ENTRADA':'Data e hora da admissão no Hospital',
        'DT_ALTA':'Data e hora da saída hospitalar',
        'DT_ENTRADA_GRUPO':'Menor data de entrada no grupo sepse',
        'sepse_expression_evolucao_med_sum':'Número de evoluções médicas que contêm palavra relacionada a sepse',
        'sepse_expression_evolucao_med_mean':'Proporção de evoluções médicas que contêm palavra relacionada a sepse',
        'primeira_data_com_sepse':'Menor data de evolução médica com palavra relacionada a sepse',
        'primeira_data_com_paliativo':'Menor data de evolução médica com cuidados paliativos',	
        'PROTOCOLO_SEPSE':'Número de prescrições com Procolo Sepse assinalado',
        'Noradrenalina':'Número de prescrições de noradrenalina (código)',
        'DT_ITEM_PRESCRITO':'Menor data de prescrição de noradrenalina/com protocolo sepse assinalado',
        'TEMPLATE_ENF': 'Número de evoluções da enfermagem com texto padrão de protocolo sepse',
        'DT_EVOLUCAO_ENF': 'Menor data de evolução da enfermagem com texto padrão de protocolo sepse',
        'DS_MOTIVO_ALTA':'Condição de saída',
    }
    cols = list(cols_names.keys())
    df_main = df_main[cols].copy()
    df_main = df_main.rename(columns=cols_names)
    
    #Outras planilhas do documento final
    evol_med_coletados = evol_med[(evol_med['campo_sepse_evolucao_med'] == True) |
                              (evol_med['CODIGO_AMARELO_EVOL'] == 1) |
                              (evol_med['sepse_expression_evolucao_med'] > 0)].copy()
    evol_med_coletados = evol_med_coletados.merge(time_between_evolucao_med_com_sepse, how='left', left_index=True, right_index=True)
    evol_med_coletados['tempo_entre_evolucao_med_com_sepse'] = evol_med_coletados['tempo_entre_evolucao_med_com_sepse'].apply(format_hours_deltatime)
    evol_med_coletados.drop('campo_sepse_evolucao_med', axis=1, inplace=True)

    evol_enf_coletados = evol_enf.drop(['DS_EVOLUCAO_ENF', 'TEMPLATE_ENF'], axis=1).copy()
    
    hemocultura_coletados = hemocultura[hemocultura['NR_ATENDIMENTO'].isin(atends_coletados)].copy()
    antibiotico_coletados = antibiotico[antibiotico['NR_ATENDIMENTO'].isin(atends_coletados)].copy()
    antibiotico_hemocultura_columns = {
        'diff_hours':'Diferença em horas entre DT_PRESCRICAO e menor data de evolução médica com palavra relacionada a sepse',
        '-48h':'DT_PRESCRICAO Até 48h antes',
        '+48h':'DT_PRESCRICAO Até 48h depois',
        '48AntesOuDepois': 'DT_PRESCRICAO até 48h antes ou depois',
    }

    for df_ in [hemocultura_coletados, antibiotico_coletados]:
        df_['diff_hours'] = df_.apply(lambda x: hemocultura_antibiotico_dentro_do_periodo(x, horarios_sepse_dict), axis=1)
        df_['-48h'] = (df_['diff_hours'] >= -48) & (df_['diff_hours'] <= 0)
        df_['+48h'] = (df_['diff_hours'] >= 0) & (df_['diff_hours'] <= 48)
        df_['48AntesOuDepois'] = df_['+48h'] | df_['-48h']
        df_.rename(columns=antibiotico_hemocultura_columns, inplace=True)
    
    prescricoes_coletados = prescricoes[prescricoes['NR_ATENDIMENTO'].isin(atends_coletados)].copy()
    prescricoes_coletados = prescricoes_coletados[prescricoes_coletados['Noradrenalina'] == 1]

    movimentacoes = movimentacoes[['NR_ATENDIMENTO', 'DT_ENTRADA_UNIDADE', 'DT_SAIDA_UNIDADE']]
    movimentacoes_coletados = movimentacoes[movimentacoes['NR_ATENDIMENTO'].isin(atends_coletados)].copy()
    
    logger.debug('Sucesso ao coletar informações para as planilhas')
    return df_main, evol_med_coletados, evol_enf_coletados, prescricoes_coletados, movimentacoes_coletados, hemocultura_coletados, antibiotico_coletados


def create_df_equipe_sepse(df_main, df_mov, df_antib, df_hemo):
    df_main, df_mov, df_antib, df_hemo = df_main.copy(), df_mov.copy(), df_antib.copy(), df_hemo.copy()
    from src.helper_functions import format_hours_deltatime
    df0 = df_main[['Número de Atendimento', 'Nome do paciente', 'Prontuário', 'Condição de saída',
                   'Data e hora da admissão no Hospital', 'Data e hora da saída hospitalar', 'Menor data de evolução médica com palavra relacionada a sepse']].copy()
    df0['Tempo de permanência hospitalar'] = df0['Data e hora da saída hospitalar'] - df0['Data e hora da admissão no Hospital']
    df0['Tempo de permanência hospitalar'] = df0['Tempo de permanência hospitalar'].astype(str).str.replace('day', 'dia')
    
    pacientes_antib = df_antib[['NR_ATENDIMENTO', 'DT_PRESCRICAO até 48h antes ou depois']].groupby('NR_ATENDIMENTO').sum()
    pacientes_antib_48 = pacientes_antib[pacientes_antib['DT_PRESCRICAO até 48h antes ou depois'] != 0].index
    df0.loc[~df0['Número de Atendimento'].isin(pacientes_antib_48), 'Menor data de evolução médica com palavra relacionada a sepse'] = float('NaN')
    
    df_antib = df_antib[df_antib['DT_PRESCRICAO até 48h antes ou depois'] == True].copy()
    df_antib['abs_diff'] = df_antib['Diferença em horas entre DT_PRESCRICAO e menor data de evolução médica com palavra relacionada a sepse'].abs()
    df_antib.sort_values('abs_diff', inplace=True)
    df_antib.drop_duplicates(subset=['NR_ATENDIMENTO'], inplace=True, keep='first')
    df1 = df0.merge(df_antib[['NR_ATENDIMENTO', 'DT_PRESCRICAO', 'DT_ADMINISTRACAO']],
                    left_on='Número de Atendimento', right_on='NR_ATENDIMENTO', how='left')
    df1.drop(columns=['NR_ATENDIMENTO'], inplace=True)
    df1.rename(columns={'DT_PRESCRICAO':'Data e hora da prescrição do antibiótico (preenchimento automático)',
                        'DT_ADMINISTRACAO':'Data e hora da administração do antibiótico (preenchimento automático)'}, inplace=True)
    
    df_hemo = df_hemo[df_hemo['DT_PRESCRICAO até 48h antes ou depois'] == True].copy()
    df_hemo['abs_diff'] = df_hemo['Diferença em horas entre DT_PRESCRICAO e menor data de evolução médica com palavra relacionada a sepse'].abs()
    df_hemo.sort_values('abs_diff', inplace=True)
    df_hemo.drop_duplicates(subset=['NR_ATENDIMENTO'], inplace=True, keep='first')
    df2 = df1.merge(df_hemo[['NR_ATENDIMENTO', 'DT_PRESCRICAO', 'DT_LIBERACAO']],
                    left_on='Número de Atendimento', right_on='NR_ATENDIMENTO', how='left')
    df2.drop(columns=['NR_ATENDIMENTO'], inplace=True)
    df2['Tempo (em horas) entre prescrição e liberação da HMC (preenchimento automático)'] =\
        ((df2['DT_LIBERACAO']-df2['DT_PRESCRICAO']).dt.total_seconds()/3600).apply(format_hours_deltatime)
    df2.rename(columns={'DT_PRESCRICAO':'Data e hora da prescrição da hemocultura (preenchimento automático)',
                        'DT_LIBERACAO':'Data e hora da liberação da hemocultura (preenchimento automático)'}, inplace=True)
    
    df3 = df2.merge(df_mov[['NR_ATENDIMENTO', 'DT_ENTRADA_UNIDADE','DT_SAIDA_UNIDADE']],
                    left_on='Número de Atendimento', right_on='NR_ATENDIMENTO', how='left') 
    df3 = df3.sort_values('DT_ENTRADA_UNIDADE').drop_duplicates(subset=['Número de Atendimento'], keep='first')\
        .drop('NR_ATENDIMENTO', axis=1).sort_index().copy()

    df3.rename(columns={'DT_ENTRADA_UNIDADE': 'Data e hora da primeira admissão à UTI',
                        'DT_SAIDA_UNIDADE': 'Data e hora da primeira saída da UTI',
                        'Menor data de evolução médica com palavra relacionada a sepse':"Data e hora do diagnóstico de sepse/ Abertura do Protocolo (preenchimento automático)"}, inplace=True) 
    
    qualidade_final_columns = ['Número de Atendimento',
                            'Nome do paciente',
                            'Prontuário',
                            'Data e hora da admissão no Hospital',
                            'Data e hora da primeira admissão à UTI', 'Data e hora da primeira saída da UTI',
                            'Tempo de permanência hospitalar', 'Condição de saída',
                            'Data e hora da saída hospitalar',
                            'Paciente com descrição de Cuidados Paliativos',
                            'Óbito >30 dias do diagnóstico de Sepse',
                            'Paciente eletivo para Indicador de Letalidade? (sim/não)',
                            'Local do diagnóstico',
                            'Data e hora do diagnóstico de sepse/ Abertura do Protocolo (preenchimento automático)',
                            'Data e hora da prescrição da hemocultura (preenchimento automático)', 'Data e hora da liberação da hemocultura (preenchimento automático)',
                            'Hemocultura antes do ATB', 'Tempo (em horas) entre prescrição e liberação da HMC (preenchimento automático)',
                            'Data e hora da prescrição do antibiótico (preenchimento automático)',
                            'Tempo entre diagnóstico e prescrição médica',
                            'Data e hora da administração do antibiótico (preenchimento automático)',
                            'Tempo em minutos - Diagnóstico e administração',
                            'Administração ATB em até 1 hora', 'Nome do antibiótico utilizado',
                            'Paciente oncológico? (sim/não)',
                            'Classificação da sepse (sepse/Choque Séptico)', 'Foco da Infecção',
                            'Comunitária/ Nosocomial', 'Teve uso de droga Vasoativa? (sim/não)',
                            'Resultado de SAPS III da admissão da UTI',
                            'Paciente teve diagnóstico de COVID nessa internação? (sim/não)']
    df3 = df3.reindex(columns=qualidade_final_columns)
    # cols_fill_na = ['Paciente com descrição de Cuidados Paliativos', 'Óbito >30 dias do diagnóstico de Sepse',
    #                 'Paciente eletivo para Indicador de Letalidade? (sim/não)', 'Local do diagnóstico',
    #                 'Tempo em minutos - Diagnóstico e administração', 'Hemocultura antes do ATB',
    #                 'Administração ATB em até 1 hora', 'Nome do antibiótico utilizado', 'Paciente oncológico? (sim/não)',
    #                 'Classificação da sepse (sepse/Choque Séptico)', 'Foco da Infecção', 'Comunitária/ Nosocomial', 'Teve uso de droga Vasoativa? (sim/não)',
    #                 'Resultado de SAPS III da admissão da UTI', 'Paciente teve diagnóstico de COVID nessa internação? (sim/não)',
    #                 ]
    # df3[cols_fill_na] = df3[cols_fill_na].fillna('')

    
    return df3


def create_excel_files(df_main, df_evol_med, df_evol_enf, df_prescricoes, df_movimentacoes, df_hemocultura, df_antibiotico):
    from src.helper_functions import apply_rtf_and_bold_expression, get_selecionados_fn_for_month, dfs_generator, sheet_names_generator
    from pandas import ExcelWriter
    from logging import getLogger
    
    logger = getLogger('standard')

    df_equipe_sepse = create_df_equipe_sepse(df_main, df_movimentacoes, df_antibiotico, df_hemocultura)
    
    n_atends_paulista = df_main[df_main['Unidade'] == 'Paulista']['Número de Atendimento'].unique().tolist()
    n_atends_vergueiro = df_main[df_main['Unidade'] == 'Vergueiro']['Número de Atendimento'].unique().tolist()
    df_main_ = df_main.copy()
    df_main_.drop('Unidade', axis=1, inplace=True)
    df_main_.drop('Condição de saída', axis=1, inplace=True)
    
    all_expressions = df_evol_med['match'].unique()
    df_evol_med['EVOLUCAO_MED'] = df_evol_med['EVOLUCAO_MED'].apply(
        lambda x: apply_rtf_and_bold_expression(x, all_expressions))
        
    options = {'strings_to_formulas' : False, 
               'strings_to_urls' : False}
    align = 'center'
    col_width = 17.4
    
    for unidade in ['Paulista', 'Vergueiro']:
        selecionados_fn = get_selecionados_fn_for_month(unidade=unidade)    
        writer = ExcelWriter(selecionados_fn, engine='xlsxwriter', engine_kwargs={'options':options})
        workbook  = writer.book
        
        index_format = workbook.add_format({
            'text_wrap': True,
            'bold':True,
            'align': align,
            'valign': 'vcenter',
            'border':True
        })
        columns_format = workbook.add_format({
            'align': align
        })
        percent_fmt = workbook.add_format({
            'num_format': '0.00%',
            'align': align
        })      
        
        n_atends_unidade = n_atends_paulista if unidade == 'Paulista' else n_atends_vergueiro
                
        # Iterador para percorrer os dfs e os nomes das planilhas
        dfs_sheet_names =zip(
            dfs_generator(df_equipe_sepse, df_main_, df_evol_med, df_evol_enf, df_prescricoes, df_movimentacoes, df_hemocultura, df_antibiotico),
            sheet_names_generator('Controle equipe sepse', 'Pacientes coletados', 'Evoluções médicas', 'Anotação padrão do Protocolo',
                                  'Prescrições Protocolo Sepse','Movimentações na UTI', 'Hemocultura', 'Antibiótico')
        )
        
        for i, (df_, sheet_name) in enumerate(dfs_sheet_names):
            # Dropando pacientes que não são da unidade
            if i <= 1:
                n_atend_col = 'Número de Atendimento'
            else:
                n_atend_col = 'NR_ATENDIMENTO'
            
            to_drop = df_.loc[~df_[n_atend_col].isin(n_atends_unidade)].index
            df_.drop(to_drop, inplace=True)
            
            if df_.empty:
                logger.warning("Planilha '%s' da %s está vazia" % (sheet_name, unidade))
                continue
            df_.to_excel(writer, sheet_name=sheet_name, index=False)
            colunas = df_.columns
            writer.sheets[sheet_name].set_column(0, len(colunas)-1, col_width, cell_format=columns_format)
            writer.sheets[sheet_name].autofilter(0, 0,  len(df_)-1, len(colunas)-1)
            for column_idx, column in enumerate(colunas):
                writer.sheets[sheet_name].write(0, column_idx, column, index_format)
                
            if sheet_name == 'Pacientes coletados':
                column_idx = list(colunas).index('Proporção de evoluções médicas que contêm palavra relacionada a sepse')
                writer.sheets[sheet_name].set_column(column_idx, column_idx, col_width, cell_format=percent_fmt)
                
        writer.save()
    
    logger.debug('Arquivos excel criados com sucesso')


if __name__ == '__main__':
    df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_, hemocultura_, antibiotico_ = gather_info_for_worksheets()
    create_excel_files(df_main_, evol_med_, evol_enf_, prescricoes_, movimentacoes_, hemocultura_, antibiotico_)
