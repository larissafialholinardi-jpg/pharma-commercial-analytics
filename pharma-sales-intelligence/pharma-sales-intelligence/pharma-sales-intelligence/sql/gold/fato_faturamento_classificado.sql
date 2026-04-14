-- ============================================================================
-- Query: Faturamento SAP — Classificação de Receita por BU e Mercado
-- Descrição: Extrai e classifica notas fiscais faturadas, aplicando
--            hierarquia de centros de lucro, BUs e mercados.
--            Calcula métricas financeiras (tabela, desconto, impostos,
--            abatimentos) reutilizando flags para evitar repetição de CASE.
--
-- Padrões utilizados:
--   - Filtro antecipado na CTE 1 para reduzir volume antes das classificações
--   - Flag is_venda calculada uma vez e reutilizada em N colunas
--   - CTEs encadeadas com responsabilidade única por camada
--   - Classificação em 3 níveis: Centro de Lucro → BU → Mercado
-- ============================================================================

WITH base_filtrada AS (
  -- ========================================================================
  -- CTE 1: Aplica filtros o mais cedo possível para reduzir dataset
  -- ========================================================================
  SELECT
    nm_setor_atividade,
    nm_divisao,
    cd_representante,
    nm_representante,
    nu_documento,
    nu_nota_fiscal,
    cd_tipo_doc_faturamento,
    cd_material,
    nu_pedido_cliente,
    nu_cnpj_cliente,
    cd_cliente,
    nm_razao_social,
    dc_material,
    dt_emissao_nf,
    dt_pedido_cliente,
    cd_tipo_venda,
    cd_empresa,
    dc_condicao_pagamento,
    qt_liquida,
    vl_bruto,
    COALESCE(vl_vendido_preco_tabela, 0) AS vl_vendido_preco_tabela,
    vl_liquido,
    vl_icms,
    vl_ipi,
    vl_pis,
    vl_cofins,
    vl_desc_com,
    vl_desc_imp,
    vl_desc_rep,
    sg_uf,
    vl_prazo,
    qt_faturada,
    qt_devolucao,
    qt_venda,
    vl_venda,
    -- Extração de data feita uma única vez para reutilização nas CTEs seguintes
    EXTRACT(MONTH FROM dt_emissao_nf) AS MES,
    EXTRACT(YEAR  FROM dt_emissao_nf) AS ANO
  FROM
    [schema].fato_sap_ov_faturada   -- tabela Gold de faturamento SAP (OV)
  WHERE
    st_estorno = 'NÃO ESTORNADO'
    AND cd_tipo_venda IN ('VND', 'SER', 'PF', 'OTE')
    AND cd_empresa IN (
      'CO_1', 'CO_2', 'CO_3', 'CO_4', 'CO_5', 'CO_6', 'CO_7'
      -- empresas do grupo filtradas por código SAP
    )
),

classificacoes AS (
  -- ========================================================================
  -- CTE 2: Calcula classificações e flags uma única vez (sem repetir CASE)
  -- Responsabilidade: derivar C_LUCRO, TIPO_DOC_FATURAMENTO e is_venda
  -- ========================================================================
  SELECT
    *,

    -- C_LUCRO: Centro de lucro derivado da combinação setor × divisão × material
    -- Regras prioritárias no topo (casos especiais de reclassificação)
    -- Regras genéricas no final (setor = BU)
    CASE
      WHEN nm_representante = '[REPRESENTANTE_ESPECIAL]'
        THEN 'CL_ESPECIAL_1'

      -- Reclassificação por material + setor (casos de transferência entre BUs)
      WHEN cd_material IN ('[MAT_1]', '[MAT_2]', '[MAT_3]')  -- substituir pelos códigos reais
        AND nm_setor_atividade = '[SETOR_A]'
        THEN 'CL_TRANSF_A'

      WHEN cd_material IN ('[MAT_4]', '[MAT_5]')
        AND nm_setor_atividade = '[SETOR_B]'
        THEN 'CL_TRANSF_B'

      -- Classificação por divisão (canal de venda)
      WHEN nm_divisao = 'NON RETAIL PRIVADO'  THEN 'CL_HOSPITALAR'
      WHEN nm_divisao = 'NON RETAIL PÚBLICO'  THEN 'CL_LICITACAO'
      WHEN nm_divisao = 'INTERNACIONAL'        THEN 'CL_EXPORTACAO'
      WHEN nm_setor_atividade = '[SETOR_INTL]' THEN 'CL_LICENCIAMENTO'

      -- Classificação por setor de atividade (regra geral: setor = BU)
      WHEN nm_setor_atividade = '[SETOR_BU_1]'  THEN 'CL_BU_1'
      WHEN nm_setor_atividade = '[SETOR_BU_2]'  THEN 'CL_BU_2'
      WHEN nm_setor_atividade = '[SETOR_BU_3]'  THEN 'CL_BU_3'
      WHEN nm_setor_atividade = '[SETOR_BU_4]'  THEN 'CL_BU_4'
      WHEN nm_setor_atividade = '[SETOR_BU_5]'  THEN 'CL_BU_5'
      WHEN nm_setor_atividade = '[SETOR_BU_6]'  THEN 'CL_BU_6'
      -- ... demais setores seguem o mesmo padrão
      ELSE NULL
    END AS C_LUCRO,

    -- TIPO_DOC_FATURAMENTO: Classifica o tipo de movimento pelo código do documento SAP
    CASE
      WHEN cd_tipo_doc_faturamento IN (
        -- códigos de documento de VENDA (prefixos Z/Y do SAP)
        'YVND','YVPN','ZFPN','ZOCV','ZOCY','ZOPN','ZOPY',
        'ZPPN','ZQPN','ZQPY','ZSER','ZVND','ZVPN','YVDN'
      ) THEN 'VENDA'

      WHEN cd_tipo_doc_faturamento IN (
        -- códigos de documento de DEVOLUÇÃO
        'YVDP','ZFDC','ZFDP','ZLDC','ZOCD','ZOCR','ZODC',
        'ZODP','ZPDP','ZQDC','ZQDP','ZVDC','ZVDM','ZVDP','ZVPC'
      ) THEN 'DEVOLUCAO'

      WHEN cd_tipo_venda = 'OTE'                  THEN 'TRIANGULAÇÃO'
      WHEN nu_pedido_cliente = 'INCINERACAO'       THEN 'DGA'
      WHEN LEFT(nu_pedido_cliente, 3) = 'DGA'     THEN 'DGA'

      WHEN cd_tipo_doc_faturamento IN (
        -- códigos de CRÉDITOS/NOTAS DE CRÉDITO
        'ZFNC','ZOT8','ZOTP','ZPFC','ZQNC','ZVNC'
      ) THEN 'CRÉDITOS'

      ELSE NULL
    END AS TIPO_DOC_FATURAMENTO,

    -- is_venda: flag binária reutilizada em múltiplas colunas de métricas
    -- Evita repetir a lista de códigos de venda em cada CASE
    CASE
      WHEN cd_tipo_doc_faturamento IN (
        'YVND','YVPN','ZFPN','ZOCV','ZOCY','ZOPN','ZOPY',
        'ZPPN','ZQPN','ZQPY','ZSER','ZVND','ZVPN','YVDN'
      ) THEN 1
      ELSE 0
    END AS is_venda

  FROM base_filtrada
),

classificacoes_BU AS (
  -- ========================================================================
  -- CTE 2.1: Mapeia Centro de Lucro → Business Unit
  -- Separado em CTE própria para permitir reutilização e clareza
  -- ========================================================================
  SELECT
    *,
    CASE
      WHEN C_LUCRO = 'CL_BU_1'        THEN 'BU PRESCRIÇÃO'
      WHEN C_LUCRO = 'CL_BU_2'        THEN 'BU GENÉRICOS'
      WHEN C_LUCRO = 'CL_BU_3'        THEN 'BU OTC'
      WHEN C_LUCRO = 'CL_BU_4'        THEN 'BU OFTALMOLOGIA'
      WHEN C_LUCRO = 'CL_BU_5'        THEN 'BU DERMATOLOGIA'
      WHEN C_LUCRO = 'CL_BU_6'        THEN 'BU MARCAS'
      WHEN C_LUCRO = 'CL_TRANSF_A'    THEN 'BU MARCAS'
      WHEN C_LUCRO = 'CL_TRANSF_B'    THEN 'BU GENÉRICOS'
      WHEN C_LUCRO = 'CL_HOSPITALAR'  THEN 'CANAL PRIVADO'
      WHEN C_LUCRO = 'CL_LICITACAO'   THEN 'CANAL PÚBLICO'
      WHEN C_LUCRO = 'CL_EXPORTACAO'  THEN 'INTERNACIONAL'
      WHEN C_LUCRO = 'CL_LICENCIAMENTO' THEN 'INTERNACIONAL'
      WHEN C_LUCRO = 'CL_ESPECIAL_1'  THEN 'CANAL ESPECIAL'
      -- ... demais centros de lucro seguem o mesmo padrão
      ELSE NULL
    END AS BU
  FROM classificacoes
),

classificacoes_mercado AS (
  -- ========================================================================
  -- CTE 2.2: Mapeia Business Unit → Mercado (agrupamento de nível superior)
  -- ========================================================================
  SELECT
    *,
    CASE
      WHEN BU = 'BU PRESCRIÇÃO'   THEN 'MERCADO PRESCRIÇÕES'
      WHEN BU = 'BU GENÉRICOS'    THEN 'MERCADO GENÉRICOS'
      WHEN BU = 'BU OTC'          THEN 'MERCADO OTC'
      WHEN BU = 'BU OFTALMOLOGIA' THEN 'MERCADO PRESCRIÇÕES'
      WHEN BU = 'BU DERMATOLOGIA' THEN 'MERCADO PRESCRIÇÕES'
      WHEN BU = 'BU MARCAS'       THEN 'MERCADO SIMILARES'
      WHEN BU = 'CANAL PRIVADO'   THEN 'MERCADO NON-RETAIL'
      WHEN BU = 'CANAL PÚBLICO'   THEN 'MERCADO NON-RETAIL'
      WHEN BU = 'CANAL ESPECIAL'  THEN 'MERCADO NON-RETAIL'
      WHEN BU = 'INTERNACIONAL'   THEN 'MERCADO NON-RETAIL'
      -- ... demais BUs seguem o mesmo padrão
      ELSE NULL
    END AS MERCADO
  FROM classificacoes_BU
),

metricas_calculadas AS (
  -- ========================================================================
  -- CTE 3: Calcula métricas financeiras reutilizando flags já calculadas
  -- is_venda e TIPO_DOC_FATURAMENTO evitam repetição de IN clauses longas
  -- ========================================================================
  SELECT
    *,
    -- Métricas de venda (condicionadas ao flag is_venda)
    CASE WHEN is_venda = 1 THEN vl_venda              ELSE 0 END AS vl_vendido,
    CASE WHEN is_venda = 1 THEN vl_vendido_preco_tabela ELSE 0 END AS FAT_TABELA,
    CASE WHEN is_venda = 1 THEN vl_desc_com            ELSE 0 END AS DESCONTO_COMERCIAL,
    CASE WHEN is_venda = 1 THEN vl_desc_imp            ELSE 0 END AS REPASSE_ICMS,
    CASE WHEN is_venda = 1 THEN vl_liquido             ELSE 0 END AS BASE_PMV,
    CASE WHEN is_venda = 1 THEN vl_prazo               ELSE 0 END AS MONTANTE_PMV,

    -- Abatimentos (condicionados ao tipo de documento)
    CASE WHEN TIPO_DOC_FATURAMENTO = 'DGA'      THEN vl_liquido * -1 ELSE 0 END AS DGA,
    CASE WHEN TIPO_DOC_FATURAMENTO = 'CRÉDITOS' THEN vl_liquido * -1 ELSE 0 END AS CREDITOS,
    CASE WHEN TIPO_DOC_FATURAMENTO = 'DEVOLUCAO'THEN vl_liquido * -1 ELSE 0 END AS DEVOLUCAO,

    -- Impostos (condicionados ao tipo de venda)
    CASE WHEN cd_tipo_venda IN ('VND','PF','SER')       THEN vl_liquido ELSE 0 END AS FAT_BRUTO,
    CASE WHEN cd_tipo_venda IN ('VND','PF','SER','OTE') THEN vl_icms    ELSE 0 END AS ICMS,
    CASE WHEN cd_tipo_venda IN ('VND','PF','SER','OTE') THEN vl_pis     ELSE 0 END AS PIS,
    CASE WHEN cd_tipo_venda IN ('VND','PF','SER','OTE') THEN vl_cofins  ELSE 0 END AS COFINS

  FROM classificacoes_mercado
)

-- ============================================================================
-- SELECT FINAL: Referencia apenas colunas já calculadas nas CTEs anteriores
-- Derivações financeiras compostas calculadas diretamente no SELECT
-- ============================================================================
SELECT
  nm_setor_atividade,
  nm_divisao,
  nm_representante,
  cd_representante,
  nu_documento,
  nu_nota_fiscal,
  cd_tipo_doc_faturamento,
  cd_material          AS CD_PRODUTO,
  dc_condicao_pagamento,
  nu_pedido_cliente,
  cd_cliente,
  nu_cnpj_cliente,
  nm_razao_social      AS CLIENTE,
  dc_material,
  dt_emissao_nf,
  cd_tipo_venda,
  cd_empresa,
  qt_liquida           AS VOLUME,
  vl_bruto,
  vl_vendido_preco_tabela,
  qt_venda,
  vl_vendido,
  vl_liquido,
  vl_icms,
  vl_ipi,
  vl_pis,
  vl_cofins,
  vl_desc_com,
  vl_desc_rep,
  vl_desc_imp,
  sg_uf,
  vl_prazo,
  MES,
  ANO,
  C_LUCRO,
  BU,
  MERCADO,
  TIPO_DOC_FATURAMENTO,
  FAT_TABELA,
  DESCONTO_COMERCIAL,
  REPASSE_ICMS,

  -- Desconto contratual: diferença entre tabela e demais componentes do faturado
  (FAT_TABELA - DESCONTO_COMERCIAL - REPASSE_ICMS - DGA - CREDITOS - DEVOLUCAO - FAT_BRUTO)
    AS DESCONTO_CONTRATUAL,

  -- Desconto total: soma de todas as formas de desconto
  (DESCONTO_COMERCIAL + REPASSE_ICMS
    + (FAT_TABELA - DESCONTO_COMERCIAL - REPASSE_ICMS - DGA - CREDITOS - DEVOLUCAO - FAT_BRUTO))
    AS DESCONTO_TOTAL,

  DGA,
  CREDITOS,
  DEVOLUCAO,
  (DGA + CREDITOS + DEVOLUCAO) AS TOTAL_DOS_ABATIMENTOS,
  FAT_BRUTO,
  ICMS,
  PIS,
  COFINS,
  BASE_PMV,
  MONTANTE_PMV,
  qt_faturada,
  qt_devolucao,
  dt_pedido_cliente

FROM metricas_calculadas
ORDER BY
  dt_emissao_nf ASC,
  CD_PRODUTO
