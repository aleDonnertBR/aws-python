# -*- coding: utf-8 -*-

#Classe que contém os dicionários que serão persistidos com os dados de pesquisas Serasa
class dicionariosSerasa(object):
    
    dicionario_status_execucao = {
                            'id' : '',
                            'status_execucao' : '',
                            'descricao_status_execucao' : '', 
                            'datahora' : '',
                            'blocos_nao_listados' : []
    }
    
    registroB49C = {
                            'numDocumentoConsultado' : "",
                            'tipoPessoaConsultado' : "",
                            'baseCons' : "",
                            'modalidade' : "",
                            'vlrConsul' : "",
                            'centroCusto' : "",
                            'codificado' : "",
                            'qtdRegistros' : "",
                            'conversa' : "",
                            'funcao' : "",
                            'tipoConsulta' : "",
                            'atualiza' : "",
                            'endTel' : "",
                            'querTel9Digitos' : "",
                            'ctaCorrente' : "",
                            'dgCtaCorrente' : "",
                            'agencia' : "",
                            'alerta' : "",
                            'logon' : "",
                            'viaInternet' : "",
                            'periodoCompro' : "",
                            'periodoEndereco' : "",
                            'qtdCompr' : "",
                            'negativos' : "",
                            'cheque' : "",
                            'dataConsul' : "",
                            'horaConsul' : "",
                            'totalReg' : "",
                            'qtdReg1' : "",
                            'codTab' : "",
                            'acessRechq' : "",
                            'temOcorrenciaRecheque' : "",
                            'reservado' : ""
    }
    
    registroB001 = {
                            'tipo_reg' : "",
                            'grafia' : "",
                            'cpf' : "",
                            'rg' : "",
                            'data_nasc' : "19000101",
                            'cod_cidade' : "",
                            'titular' : "",
                            'ccf_ind' : "",        
                            'link_n' : "",
                            'situaca_antiga' : "",
                            'dt_atual' : "19000101",
                            'reservado' : "",
                            'indic_erro' : "",
                            'excluir_graf' : "",
                            'nova_situacao' : ""
                        }  
    
    registroB001_outros = {
                        'tipo_reg' : [],
                        'grafia' : [],
                        'cpf' : [],
                        'rg' : [],
                        'data_nasc' : [],
                        'cod_cidade' : [],
                        'titular' : [],
                        'ccf_ind' : [],        
                        'link_n' : [],
                        'situaca_antiga' : [],
                        'dt_atual' : [],
                        'reservado' : [],
                        'indic_erro' : [],
                        'excluir_graf' : [],
                        'nova_situacao' : []
                    }  
                    
    registroA900 = {
                        'tipo_reg' :  [],
                        'codigo' :  [],
                        'mens_reduz' :  [],
                        'mens_compl' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB002 = {
                        'tipo_reg' :  [],
                        'filler_1' :  [],
                        'atualiza' :  [],
                        'data_nasc' :  [],
                        'nome_mae' :  [],
                        'sexo' :  [],
                        'tipo_doc' :  [],
                        'numdoc' :  [],
                        'orgao_emis' :  [],
                        'data_emis' :  [],
                        'uf_emissor' :  [],
                        'filler_2' :  [],
                        'qtd_registros' : 0
    }
    
    registroB003 = {
                        'tipo_reg' :  [],
                        'estado_civil' : [],
                        'depend' :  [],
                        'escolar' :  [],
                        'mun_nasc' :  [],
                        'uf_nasc' :  [],
                        'cpf_conjuge' :  [],
                        'ddd_res' :  [],
                        'fone_res' :  [],
                        'ddd_coml' :  [],
                        'fone_coml' :  [],
                        'ramal' :  [],
                        'celular' :  [],
                        'ddd_cel' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB004 = {
                        'tipo_reg' :  [],
                        'logradouro' :  [],
                        'numero' :  [],
                        'complemento':  [],
                        'bairro' :  [],
                        'cidade' :  [],
                        'uf' :  [],
                        'cep' :  [],
                        'desde' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
                    
    registroB005 = {
    
                        'tipo_reg' :  [],   
                        'ocupacao' :  [],
                        'renda' :  [],
                        'ct_serie' :  [],
                        'num_ct' :  [],
                        'filler_1' :  [],
                        'partic' :  [],
                        'filler_2' :  [],
                        'qtd_registros' : 0
        }
    
    registroB006 = {
                        'tipo_reg' :  [],
                        'empresa' :  [],
                        'desde' :  [],
                        'tipo_moeda' :  [],
                        'profissão' :  [],
                        'cargo' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB352 = {
                        'tipo_reg' :  [],
                        'empresa' :  [],
                        'cnpj' :  [],
                        'partic' :  [],
                        'estado' :  [],
                        'situacao' :  [],
                        'dt_ini_partic' :  [],
                        'dt_ult_atu' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB35P = {
                        'tipo_reg' :  [],
                        'restri' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB353 = {
                        'tipo_reg' :  [],
                        'qtde_total_credito' :  [],
                        'data_atual' :  [],
                        'qtde_atual' :  [],
                        'qtde_mes_credito_1' :  [],
                        'qtde_mes_credito_2' :  [],
                        'qtde_mes_credito_3' :  [],
                        'qtde_total_cheque' :  [],
                        'qtdeatual' :  [],
                        'qtde_mes_cheque_1' :  [],
                        'qtde_mes_cheque_2' :  [],
                        'qtde_mes_cheque_3' :  [],
                        'posicao' :  [],
                        'data_ficad' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
                        
    registroB354 = {
                        'tipo_reg' :  [],
                        'dt_ocorr' :  [],
                        'origem' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
        
    registroB357  = {
                        'tipo_reg' :  [],
                        'qtde_total' :  [],
                        'descricao' :  [],
                        'data_menor' :  [],
                        'data_maior' :  [],
                        'tip_moeda' :  [],
                        'vlr_ultima' :  [],
                        'origem' :  [],
                        'filial' :  [],
                        'tip_pefin' :  [],
                        'val_total' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB358 = {
                        'tipo_reg' :  [],
                        'tipo_pefin' :  [],
                        'modalidade' :  [],
                        'tipo_ocor' :  [],
                        'chave_cadus' :  [],
                        'filler_1' :  [],
                        'ult_ocor' :  [],
                        'dt_ocorr' :  [],
                        'sigla_mod' :  [],
                        'principal' :  [],
                        'tipo_moeda' :  [],
                        'valor' :  [],
                        'contrato' :  [],
                        'origem' :  [],
                        'filial' :  [],
                        'qtde_ocorr' :  [],
                        'cod_banco' :  [],
                        'subjud_x' :  [],
                        'uf' :  [],
                        'filler_2' :  [],
                        'cont_contrato' :  [],
                        'filler_3' :  [],
                        'qtd_registros' : 0
    }
    
    registroB359 = {
                        'tipo_reg' :  [],
                        'qtde_total' :  [],
                        'descricao' :  [],
                        'data_menor' :  [],
                        'data_maior' :  [],
                        'tip_moeda' :  [],
                        'vlr_ultima' :  [],
                        'origem' :  [],
                        'filial' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB360 = {
                        'tipo_reg' :  [],
                        'dt_ocorr' :  [],
                        'num_cheque' :  [],
                        'alinea' :  [],
                        'qtde_cheque' :  [],
                        'tipo_moeda' :  [],
                        'valor' :  [],
                        'banco' :  [],
                        'agencia' :  [],
                        'cidade' :  [],
                        'uf' :  [],
                        'qtde_ocorr' :  [],
                        'tipo_conta' :  [],
                        'conta' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB361 = {
                        'tipo_reg' :  [],
                        'qtde_total' :  [],
                        'descricao' :  [],
                        'data_menor' :  [],
                        'data_maior' :  [],
                        'tip_moeda' :  [],
                        'valor' :  [],
                        'cidade' :  [],
                        'uf' :  [],
                        'val_total' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB362 = {
                        'tipo_reg' :  [],
                        'dt_ocorr' :  [],
                        'tipo_moeda' :  [],
                        'valor' :  [],
                        'cartorio' :  [],
                        'cidade' :  [],
                        'uf' :  [],
                        'qtde_ocorr' :  [],
                        'subjudice' :  [],
                        'data' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB363 = {
                        'tipo_reg' :  [],
                        'qtde_total' :  [],
                        'descricao' :  [],
                        'data_menor' :  [],
                        'data_maior' :  [],
                        'tip_moeda' :  [],
                        'valor' :  [],
                        'natureza' :  [],
                        'uf' :  [],
                        'val_total' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB364 = {
                        'tipo_reg' :  [],
                        'dt_ocorr' :  [],
                        'natureza' :  [],
                        'principal' :  [],
                        'tipo_moeda' :  [],
                        'valor' :  [],
                        'distribuid' :  [],
                        'vara' :  [],
                        'cidade' :  [],
                        'uf' :  [],
                        'qtde_ocorr' :  [],
                        'subjudice' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB365 = {
                        'tipo_reg' :  [],
                        'total_ocor' :  [],
                        'descricao' :  [],
                        'data_menor' :  [],
                        'data_maior' :  [],
                        'empresa' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
                        
    registroB366 = {
                        'tipo_reg' :  [],
                        'data_ocor' :  [],
                        'tipo_ocor' :  [],
                        'cnpj_pie' :  [],
                        'empresa' :  [],
                        'total_ocor' :  [],
                        'qualif' :  [],
                        'vara_civil' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
                        }
    
    registroB367 = {
                        'tipo_reg' :  [],
                        'qtde_total' :  [],
                        'descricao' :  [],
                        'data_menor' :  [],
                        'data_maior' :  [],
                        'tip_moeda' :  [],
                        'vlr_ultima' :  [],
                        'origem' :  [],
                        'local' :  [],
                        'val_total' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB368 = {
                        'tipo_reg' :  [],
                        'dt_ocorr' :  [],
                        'natureza' :  [],
                        'tipo_moeda' :  [],
                        'valor' :  [],
                        'titulo' :  [],
                        'origem' :  [],
                        'local' :  [],
                        'qtde_ocorr' :  [],
                        'tipo_ocor' :  [],
                        'chave_cadus' :  [],
                        'filler_1' :  [],
                        'nova_natur' :  [],
                        'subjudice' :  [],
                        'serie_cadus' :  [],
                        'filler_2' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB370 = {
                        'tipo_reg' :  [],
                        'tipo_de_reg' :  [],
                        'ddd_do_tel' :  [],
                        'nro_fone' :  [],
                        'endereco' :  [],
                        'bairro' :  [],
                        'cep' :  [],
                        'qtd_registros' : 0
    }
    
    registroB370_TIPO2 = {
                        'tipo_reg' :  [],
                        'tipo_de_reg' :  [],
                        'cidade' :  [],
                        'uf' :  [],
                        'nome' :  [],
                        'dt_atualiza' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
        
    }
    
    registroB376 = {
                        'tipo_reg' :  [],
                        'tipo_de_reg' :  [],
                        'tipo_doc' :  [],
                        'desc_tp_doc' :  [],
                        'nro_doc' :  [],
                        'data_ocorr' :  [],
                        'motivo_ocorr' :  [],
                        'ddd_contato' :  [],
                        'fone_contato' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB376_CELULAR = {
                        'tipo_reg' :  [],
                        'tipo_de_reg' :  [],
                        'tipo_doc' :  [],
                        'desc_tp_doc' :  [],
                        'nro_doc' :  [],
                        'data_ocorr' :  [],
                        'motivo_ocorr' :  [],
                        'ddd_contato' :  [],
                        'fone_contato' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    
    registroB376_TIPO2 = {
                        'tipo_reg' :  [],
                        'tipo_de_reg' :  [],
                        'mensagem' :  [],
                        'mens_reduzida' :  [],
                        'filler' :  [],
                        'qtd_registros' : 0
    }
    
    registroB280 = {
	
						'tipo_reg' :  '',
						'tipo' :  '',
						'score' :  0,
						'range' :  '',
						'taxa' :  0,
						'mensagem' :  '',
						'codigo' :  '',
						'filler' :  '',
						'qtd_registros' : 0
    }
	
    registroP002 = {
						'tipo_reg' :  [],
						'primeiro_cod' :  [],
						'primeira_chave' :  [],
						'segundo_cod' :  [],
						'segunda_chave' :  [],
						'terceiro_cod' :  [],
						'terceira_chave' :  [],
						'quarto_cod' :  [],
						'quarta_chave' :  [],
						'filler' :  [],
						'qtd_registros' : 0
    }	    