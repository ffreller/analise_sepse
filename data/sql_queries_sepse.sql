-- Dados Básicos
select         ap.cd_estabelecimento, 
               pf.nr_prontuario,
               pf.cd_pessoa_fisica cd_pessoa_fisica,
			   pf.nm_pessoa_fisica,
               ap.nr_atendimento,
               ap.dt_entrada,
               pf.dt_nascimento,
               pf.ie_sexo,
               pf.nr_seq_cor_pele,
               cl.ds_cor_pele,
               acc.cd_convenio,
               con.ds_convenio,
               ap.ie_tipo_atendimento,
               to_number(ap.cd_medico_resp) id_medico,
               tasy.obter_clinica_atendimento(ap.nr_atendimento) ds_clinica,
               substr(tasy.obter_valor_dominio(12,
                                                             ap.ie_tipo_atendimento),
                      1,
                      254) ds_tipo_atendimento,
               ap.dt_alta,
               ap.cd_motivo_alta,
               tasy.obter_motivo_alta_atend(ap.nr_atendimento) AS ds_motivo_alta,
               (SELECT LISTAGG(dd.cd_doenca, ';') WITHIN GROUP (ORDER BY dd.dt_liberacao, dd.cd_doenca)
                  FROM tasy.diagnostico_doenca dd
                 WHERE dd.nr_atendimento = ap.nr_atendimento
                  and dd.dt_liberacao is not null) cd_doenca,
                  gse.NR_SEQ_GRUPO,
                  gse.DS_GRUPO,
                  gse.DT_ENTRADA DT_ENTRADA_GRUPO,
                  gse.DT_SAIDA DT_SAIDA_GRUPO,
				  ram.dt_liberacao dt_codigo_amarelo
          from tasy.atendimento_paciente ap,
               tasy.pessoa_fisica pf,
               tasy.cor_pele      cl,
               tasy.atend_categoria_convenio acc,
               tasy.categoria_convenio cc,
               tasy.convenio con,
         (select ap.NR_ATENDIMENTO,
             pf.CD_PESSOA_FISICA,
             pf.NM_PESSOA_FISICA,
             pga.NR_SEQ_GRUPO,
             pg.NR_SEQUENCIA,
             pg.DS_GRUPO,
             pga.DT_ENTRADA,
             pga.DT_SAIDA
      from   tasy.ATENDIMENTO_PACIENTE ap,
             tasy.PAC_GRUPO_ATEND pga,   
             tasy.PESSOA_FISICA pf,
             tasy.PAC_GRUPO pg       
        where 1=1
        and ap.CD_PESSOA_FISICA = pga.CD_PESSOA_FISICA
        and ap.Nr_Atendimento = pga.nr_atendimento        
        and pga.cd_pessoa_fisica = pf.cd_pessoa_fisica
        and pga.NR_SEQ_GRUPO = pg.NR_SEQUENCIA
        and ap.cd_estabelecimento = 1
        and pg.nr_sequencia = 4) gse,
    ( select ehr_registro.nr_atendimento,
        ehr_registro.dt_liberacao
   from tasy.ehr_elemento,
        tasy.ehr_reg_elemento,
        tasy.ehr_reg_template,
        tasy.ehr_registro,
        tasy.ehr_template_conteudo        
WHERE 1=1
    AND ehr_elemento.nr_sequencia = 9003897
    AND ehr_reg_elemento.nr_seq_elemento = ehr_elemento.nr_sequencia 
    AND ehr_reg_template.nr_sequencia = ehr_reg_elemento.nr_seq_reg_template     
    AND ehr_reg_template.nr_seq_reg = ehr_registro.nr_sequencia     
    AND ehr_template_conteudo.nr_seq_template = ehr_reg_template.nr_seq_template
    AND ehr_template_conteudo.nr_sequencia = ehr_reg_elemento.nr_seq_temp_conteudo
    AND ehr_reg_elemento.ds_resultado = 'SEPSE'
    AND ehr_registro.dt_inativacao IS NULL
    AND ehr_registro.dt_liberacao IS NOT NULL) ram
         where 1 = 1
           and ap.cd_estabelecimento in (1,17)
		   and ap.ie_tipo_atendimento = 1
		   and ap.dt_cancelamento is null
           and ap.dt_alta >= to_date('DATE_TO_REPLACE_START','dd/mm/rrrr') 
           and ap.dt_alta < to_date('DATE_TO_REPLACE_END','dd/mm/rrrr')
           and ap.cd_pessoa_fisica = pf.cd_pessoa_fisica
           and pf.nr_seq_cor_pele = cl.nr_sequencia(+)
           and tasy.obter_atecaco_atendimento(ap.nr_atendimento) = acc.nr_seq_interno
           and acc.cd_convenio  =   cc.cd_convenio
           and acc.cd_categoria =   cc.cd_categoria
           and cc.cd_convenio   =   con.cd_convenio
           and ap.nr_atendimento = gse.nr_atendimento(+)
       and ap.nr_atendimento = ram.nr_atendimento(+)
	   


-- Evolução Médica
select ap.cd_estabelecimento,
       evp.nr_atendimento,
       evp.dt_evolucao,
       evp.dt_liberacao,
       evp.ds_evolucao
  from tasy.evolucao_paciente evp,
       tasy.atendimento_paciente ap
 where 1=1 
   and evp.nr_atendimento = ap.nr_atendimento
   and ap.cd_estabelecimento in (1,17)
   and ap.ie_tipo_atendimento = 1
   and ap.dt_cancelamento is null
   and ap.dt_alta >= to_date('DATE_TO_REPLACE_START','dd/mm/rrrr')
   and ap.dt_alta < to_date('DATE_TO_REPLACE_END','dd/mm/rrrr')
   and evp.dt_liberacao is not null
   and evp.dt_inativacao is null
   and evp.ie_tipo_evolucao = '1'

   
-- Evolução Enfermagem
select ap.cd_estabelecimento,
       evp.nr_atendimento,
       evp.dt_evolucao,
       evp.dt_liberacao,
       evp.ds_evolucao,
       sa.ds_setor_atendimento,
       sa.nm_curto
  from tasy.evolucao_paciente evp,
       tasy.atendimento_paciente ap,
       tasy.setor_atendimento    sa
 where 1=1 
   and evp.nr_atendimento = ap.nr_atendimento
   and evp.cd_setor_atendimento = sa.cd_setor_atendimento
   and ap.cd_estabelecimento  in (1,17)
   and ap.ie_tipo_atendimento = 1
   and ap.dt_cancelamento is null
   and ap.dt_alta >= to_date('DATE_TO_REPLACE_START','dd/mm/rrrr')
   and ap.dt_alta < to_date('DATE_TO_REPLACE_END','dd/mm/rrrr')
   and evp.dt_liberacao is not null
   and evp.dt_inativacao is null
   and evp.ie_tipo_evolucao = '3'
   
   
-- Movimentações Setores
select cd_estabelecimento,
       nr_atendimento,
       dt_entrada_unidade,
       dt_saida_unidade
  from (select cd_estabelecimento,
               nr_atendimento,
               first_value(dt_entrada_unidade) over(partition by nr_atendimento, ordem order by dt_entrada_unidade asc nulls first) dt_entrada_unidade,
               first_value(dt_saida_unidade) over(partition by nr_atendimento, ordem order by dt_saida_unidade desc nulls first) dt_saida_unidade
          from (select a.cd_estabelecimento,
                       a.nr_atendimento,
                       a.dt_entrada_unidade,
                       a.dt_saida_unidade,
                       a.cd_classif_setor,
                       sum(a.valor) over(partition by a.nr_atendimento order by a.dt_entrada_unidade, a.dt_saida_unidade desc nulls last) ordem
                  from (select ap.cd_estabelecimento as cd_estabelecimento,
                               apu.nr_atendimento as nr_atendimento,
                               apu.cd_setor_atendimento id_setor,
                               tasy.obter_nome_setor(apu.cd_setor_atendimento) as ds_setor,
                               apu.cd_unidade_basica as ds_leito,
                               apu.dt_entrada_unidade as dt_entrada_unidade,
                               apu.dt_saida_unidade as dt_saida_unidade,
                               sa.cd_classif_setor,
                               case
                                 when sa.cd_classif_setor =
                                      lag(sa.cd_classif_setor, 1, '#')
                                  over(partition by ap.nr_atendimento order by
                                           apu.dt_entrada_unidade,
                                           apu.dt_saida_unidade desc nulls last) then
                                  0
                                 else
                                  1
                               end valor
                          from tasy.atendimento_paciente   ap,
                               tasy.atend_paciente_unidade apu,
                               tasy.unidade_atendimento    ua,
                               tasy.setor_atendimento      sa
                         where 1 = 1
                           and ap.cd_estabelecimento in (1, 17)
                           and ap.ie_tipo_atendimento = 1
						   and ap.dt_cancelamento is null
                           and ap.dt_alta >= to_date('DATE_TO_REPLACE_START', 'dd/mm/rrrr')
                           and ap.dt_alta <  to_date('DATE_TO_REPLACE_END', 'dd/mm/rrrr')
                           and ap.nr_atendimento = apu.nr_atendimento
                           and apu.cd_setor_atendimento =
                               ua.cd_setor_atendimento
                           and apu.cd_unidade_basica = ua.cd_unidade_basica
                           and apu.ie_passagem_setor in ('N', 'L')
                           and ua.cd_setor_atendimento =
                               sa.cd_setor_atendimento
                         order by apu.nr_atendimento, apu.dt_entrada_unidade) a)
         where cd_classif_setor = 4)
 group by cd_estabelecimento,
          nr_atendimento,
          dt_entrada_unidade,
          dt_saida_unidade
order by 2,3

-- Prescrição Medicamentos
Select ap.cd_estabelecimento,
       ap.nr_atendimento,
       ap.dt_entrada,
       pm.nr_prescricao,
       pm.dt_prescricao DT_ITEM_PRESCRITO,
       pmt.dt_lib_enfermagem DT_ITEM_EXECUTADO,
       'Medicação' DS_TIPO_PRESCRICAO,
       m.ds_material DS_PRESCRICAO,
       map.nr_sequencia id_execucao,
       map.qt_executada QT_ITEM,
       m.cd_material,
       tpt.CD_TIPO_PROTOCOLO
       ,tpt.DS_TIPO_PROTOCOLO
       ,pt.CD_PROTOCOLO
       ,pt.NM_PROTOCOLO       
  from tasy.atendimento_paciente ap,
       tasy.prescr_medica pm,
       tasy.prescr_material pmt,
       tasy.material_atend_paciente map,
       tasy.material m,
       tasy.protocolo_medicacao ptm,
       (select * from tasy.PROTOCOLO pt where pt.CD_PROTOCOLO in (221, 217, 211, 555, 571, 706, 603)) pt, 
       (select * from tasy.TIPO_PROTOCOLO tpt where tpt.CD_TIPO_PROTOCOLO in (88,165, 163, 172)) tpt  
where 1 = 1
  and ap.nr_atendimento = pm.nr_atendimento 
  and pm.dt_liberacao is not null
  and pm.nr_prescricao = pmt.nr_prescricao   
  and pmt.nr_prescricao = map.nr_prescricao
  and pmt.nr_sequencia = map.nr_sequencia_prescricao
  and pmt.cd_material = m.cd_material  
  and (pm.CD_PROTOCOLO = ptm.CD_PROTOCOLO(+) and pm.NR_SEQ_PROTOCOLO = ptm.NR_SEQUENCIA(+))
  and (ptm.CD_PROTOCOLO = pt.CD_PROTOCOLO(+))
  and (pt.CD_TIPO_PROTOCOLO = tpt.CD_TIPO_PROTOCOLO(+))
  and ap.cd_estabelecimento in (1,17)
  and ap.ie_tipo_atendimento = 1
  and ap.dt_alta >= to_date('DATE_TO_REPLACE_START','dd/mm/rrrr')
  and ap.dt_alta  < to_date('DATE_TO_REPLACE_END','dd/mm/rrrr')
  and ap.dt_cancelamento is null
  

-- Hemocultura
Select ap.cd_estabelecimento,
       ap.nr_atendimento,
       pm.nr_prescricao,
       pm.dt_prescricao,
       pm.dt_liberacao,
       p.ds_procedimento,
       pme.nm_medicacao nm_protocolo_medicacao
  from tasy.atendimento_paciente ap
  join tasy.prescr_medica pm on (ap.nr_atendimento = pm.nr_atendimento)
  join tasy.prescr_procedimento ppr on (pm.nr_prescricao = ppr.nr_prescricao)
  join tasy.procedimento p on (ppr.cd_procedimento = p.cd_procedimento)
  left join tasy.protocolo_medicacao pme on (pm.cd_protocolo = pme.cd_protocolo and  pm.nr_seq_protocolo = pme.nr_sequencia)
 where 1 = 1
   and ap.cd_estabelecimento in (1,17)
   and ap.ie_tipo_atendimento = 1
   and ap.dt_cancelamento is null
   and ap.dt_alta >= to_date('DATE_TO_REPLACE_START','dd/mm/rrrr')
   and ap.dt_alta < to_date('DATE_TO_REPLACE_END','dd/mm/rrrr')
   and pm.dt_liberacao is not null
   and UPPER(p.ds_procedimento) like '%HEMOCUL%'
   /*and p.cd_procedimento = 40310256 */
 group by ap.cd_estabelecimento,
          ap.nr_atendimento,
          pm.nr_prescricao,
          pm.dt_prescricao,
          pm.dt_liberacao,
          p.ds_procedimento,
          pme.nm_medicacao


-- Antibiótico
Select ap.cd_estabelecimento,
       ap.nr_atendimento,
       pm.nr_prescricao,
       pm.dt_prescricao,
       pm.dt_liberacao,
       vd.ds_valor_dominio objetivo_uso,
       ip.ds_intervalo,
       cmat.ds_medicamento ds_principio_ativo,
       mat.cd_material cd_material_prescrito,
       mat.ds_material ds_material_prescrito,
       map.dt_atendimento dt_administracao,
       mata.cd_material cd_material_administrado,
       mata.ds_material ds_material_administrado       
  from tasy.atendimento_paciente ap
  join tasy.prescr_medica pm on (ap.nr_atendimento = pm.nr_atendimento)
  join tasy.prescr_material ppr on (pm.nr_prescricao = ppr.nr_prescricao)
  join tasy.material mat on (ppr.cd_material = mat.cd_material)
  left join tasy.material matc on ( mat.cd_material = matc.cd_material_conta)
  join tasy.cih_medicamento cmat on (mat.cd_medicamento = cmat.cd_medicamento)
  join tasy.material_atend_paciente map on (ppr.nr_prescricao = map.nr_prescricao and ppr.nr_sequencia = map.nr_sequencia_prescricao)
  join tasy.material mata on (map.cd_material_prescricao = mata.cd_material)
  left join tasy.intervalo_prescricao ip on (ppr.cd_intervalo = ip.cd_intervalo)
  left join (select vl_dominio, ds_valor_dominio FROM TASY.valor_dominio WHERE CD_DOMINIO = 1280) vd on (ppr.ie_objetivo = vd.vl_dominio)
where 1 = 1
   and ap.cd_estabelecimento in (1,17)
   and ap.ie_tipo_atendimento = 1
   and ap.dt_cancelamento is null   
   and pm.dt_liberacao is not null
   and ap.dt_alta >= to_date('DATE_TO_REPLACE_START','dd/mm/rrrr')
   and ap.dt_alta < to_date('DATE_TO_REPLACE_END','dd/mm/rrrr')
   and ppr.ie_objetivo is not null
		  
		  
		  