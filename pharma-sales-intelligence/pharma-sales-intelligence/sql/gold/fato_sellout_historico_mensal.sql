-- ============================================================================
-- Pipeline: fato_sellout_historico_mensal
-- Descrição: Rebuild mensal completo da tabela Gold de sell-out histórico.
--            Unifica dados de auditoria de mercado (Close-Up/CDD) de 4 fontes:
--            farmácia mensal, farmácia histórica (adhoc), transferência mensal
--            e transferência histórica.
--
-- Estratégia de carga: DELETE + INSERT (full rebuild mensal)
--   Justificativa: a fonte de auditoria retroage dados de meses anteriores,
--   tornando a carga incremental inadequada — um rebuild garante consistência.
--
-- Padrões utilizados:
--   - VALUES inline como tabela de mapeamento (sap_map) — evita join com tabela auxiliar
--   - UNION ALL de 4 fontes com tag de origem (tabela_origem, canal_origem_info)
--   - Filtro anti-duplicação via NOT IN (SELECT DISTINCT meses já na tabela principal)
--     para integrar histórico adhoc sem sobrescrever dados já consolidados
--   - Exclusão de combinações distribuidor × PDV via NOT (...OR...) — regra de negócio
--     para evitar dupla contagem em transferências de grandes redes
--   - COALESCE para fallback de código SAP ajustado
--   - CONCAT_WS para geração de chaves relacionais (sku_id, chave_cliente)
--
-- Frequência: mensal (executada após fechamento da auditoria)
-- Destino: tabela Gold Delta Lake de sell-out histórico mensal
-- ============================================================================

-- Rebuild completo: deleta antes de inserir (garante consistência com retroações)
DELETE FROM [schema_destino].fato_sellout_historico_mensal;

INSERT INTO [schema_destino].fato_sellout_historico_mensal

WITH sap_map AS (
  -- -------------------------------------------------------------------------
  -- Mapeamento inline de códigos de apresentação → código SAP ajustado
  -- Usado quando o cd_sap da dimensão de produtos não reflete o SKU correto
  -- Evita criação de tabela auxiliar para um mapeamento estático e pequeno
  -- -------------------------------------------------------------------------
  SELECT cd_apresentacao, cd_sap_ajustado
  FROM (VALUES
    (39058,50191),(19383,50009),(19445,50190),(124505,15557),(124506,15556),
    (124507,15555),(19429,50154),(19447,50193),(19451,50221),(19463,50274),
    (19515,50486),(19528,50607),(19816,2143),(19901,15036),(20017,15180),
    (20025,15187),(20063,15200),(33323,50328),(40222,50970),(40845,50262),
    (40873,50261),(41418,50257),(41419,50334),(42645,2121),(51283,50256),
    (53341,15466),(72999,50089),(131369,51198),(185094,51577),(208036,63010),
    (269854,50153)
  ) AS t(cd_apresentacao, cd_sap_ajustado)
),

base_bruta AS (

  -- -------------------------------------------------------------------------
  -- FONTE 1: Farmácia — tabela mensal principal (dados consolidados)
  -- -------------------------------------------------------------------------
  SELECT
    'mensal'   AS tabela_origem,
    'farmacia' AS canal_origem_info,
    m.dt_periodo, m.cd_apresentacao, m.cd_pdv, m.cd_distribuidor,
    m.qt_unidade, m.vl_reais_preco_fabrica,
    m.vl_reais_preco_com_desconto, m.vl_reais_preco_consumidor,
    p.cd_ean, p.cd_sap, p.dc_divisao,
    pdv.nu_cnpj_pdv, pdv.dc_pdv, pdv.sg_uf, pdv.tp_assoc_franquia,
    c.dc_subcanal

  FROM [schema_auditoria].fato_cdd_demanda_mensal        m
  INNER JOIN [schema_auditoria].dm_produtos_corp         p   ON m.cd_apresentacao = p.cd_apresentacao
  INNER JOIN [schema_auditoria].dm_pdv_mensal            pdv ON m.cd_pdv          = pdv.cd_pdv
  INNER JOIN [schema_auditoria].dm_canal_mensal          c   ON pdv.cd_subcanal   = c.cd_subcanal
                                                             AND c.cd_canal = 1
  WHERE p.dc_divisao IN ('BU_A', 'BU_B', 'BU_C', 'BU_D')

  UNION ALL

  -- -------------------------------------------------------------------------
  -- FONTE 2: Farmácia — histórico adhoc (meses não cobertos pela mensal)
  -- Filtro anti-duplicação: exclui meses já presentes na tabela mensal principal
  -- -------------------------------------------------------------------------
  SELECT
    'historico' AS tabela_origem,
    'farmacia'  AS canal_origem_info,
    m.dt_periodo, m.cd_apresentacao, m.cd_pdv, m.cd_distribuidor,
    m.qt_unidade, m.vl_reais_preco_fabrica,
    m.vl_reais_preco_com_desconto, m.vl_reais_preco_consumidor,
    p.cd_ean, p.cd_sap, p.dc_divisao,
    pdv.nu_cnpj_pdv, pdv.dc_pdv, pdv.sg_uf, pdv.tp_assoc_franquia,
    c.dc_subcanal

  FROM [schema_auditoria_adhoc].fato_cdd_demanda_mensal  m
  INNER JOIN [schema_auditoria].dm_produtos_corp         p   ON m.cd_apresentacao = p.cd_apresentacao
  INNER JOIN [schema_auditoria].dm_pdv_mensal            pdv ON m.cd_pdv          = pdv.cd_pdv
  INNER JOIN [schema_auditoria].dm_canal_mensal          c   ON pdv.cd_subcanal   = c.cd_subcanal
                                                             AND c.cd_canal = 1
  WHERE p.dc_divisao IN ('BU_A', 'BU_B', 'BU_C', 'BU_D')
    AND m.dt_periodo >= '2024-01-01'
    -- Exclui meses já consolidados na tabela mensal principal
    AND DATE_TRUNC('month', m.dt_periodo) NOT IN (
        SELECT DISTINCT DATE_TRUNC('month', dt_periodo)
        FROM [schema_auditoria].fato_cdd_demanda_mensal
    )

  UNION ALL

  -- -------------------------------------------------------------------------
  -- FONTE 3: Transferência — tabela mensal principal (últimos 24 meses)
  -- Exclusão de combinações distribuidor × PDV para evitar dupla contagem
  -- em grandes redes que recebem de distribuidores específicos
  -- -------------------------------------------------------------------------
  SELECT
    'mensal'        AS tabela_origem,
    'transferencia' AS canal_origem_info,
    m.dt_periodo, m.cd_apresentacao, m.cd_pdv, m.cd_distribuidor,
    m.qt_unidade, m.vl_reais_preco_fabrica,
    m.vl_reais_preco_com_desconto, m.vl_reais_preco_consumidor,
    p.cd_ean, p.cd_sap, p.dc_divisao,
    pdv.nu_cnpj_pdv, pdv.dc_pdv, pdv.sg_uf, pdv.tp_assoc_franquia,
    NULL AS dc_subcanal   -- transferência não tem subcanal

  FROM [schema_auditoria].fato_cdd_transferencia_mensal  m
  INNER JOIN [schema_auditoria].dm_produtos_corp         p   ON m.cd_apresentacao = p.cd_apresentacao
  INNER JOIN [schema_auditoria].dm_distribuidor_mensal   d   ON m.cd_distribuidor  = d.cd_distribuidor
  INNER JOIN [schema_auditoria].dm_pdv_mensal            pdv ON m.cd_pdv           = pdv.cd_pdv

  WHERE DATE_TRUNC('month', m.dt_periodo) >= ADD_MONTHS(DATE_TRUNC('month', CURRENT_DATE), -24)
    AND p.dc_divisao IN ('BU_A', 'BU_B', 'BU_C', 'BU_D')
    -- Exclui pares distribuidor × PDV que causam dupla contagem
    AND NOT (
      (d.dc_grupo_distribuidor ILIKE '%DIST_A%' AND pdv.dc_pdv LIKE '%REDE_X%')
      OR (d.dc_grupo_distribuidor ILIKE '%DIST_A%' AND pdv.dc_pdv LIKE '%REDE_Y%')
      OR (d.dc_grupo_distribuidor ILIKE '%DIST_A%' AND pdv.dc_pdv LIKE '%REDE_Z%')
      OR (d.dc_grupo_distribuidor ILIKE '%DIST_B%' AND pdv.dc_pdv LIKE '%REDE_W%')
    )

  UNION ALL

  -- -------------------------------------------------------------------------
  -- FONTE 4: Transferência — histórico adhoc (meses não cobertos pela mensal)
  -- Mesma lógica anti-duplicação e anti-dupla-contagem das fontes anteriores
  -- -------------------------------------------------------------------------
  SELECT
    'historico'     AS tabela_origem,
    'transferencia' AS canal_origem_info,
    m.dt_periodo, m.cd_apresentacao, m.cd_pdv, m.cd_distribuidor,
    m.qt_unidade, m.vl_reais_preco_fabrica,
    m.vl_reais_preco_com_desconto, m.vl_reais_preco_consumidor,
    p.cd_ean, p.cd_sap, p.dc_divisao,
    pdv.nu_cnpj_pdv, pdv.dc_pdv, pdv.sg_uf, pdv.tp_assoc_franquia,
    NULL AS dc_subcanal

  FROM [schema_auditoria_adhoc].fato_cdd_transferencia_mensal m
  INNER JOIN [schema_auditoria].dm_produtos_corp             p   ON m.cd_apresentacao = p.cd_apresentacao
  INNER JOIN [schema_auditoria].dm_distribuidor_mensal       d   ON m.cd_distribuidor  = d.cd_distribuidor
  INNER JOIN [schema_auditoria].dm_pdv_mensal                pdv ON m.cd_pdv           = pdv.cd_pdv

  WHERE p.dc_divisao IN ('BU_A', 'BU_B', 'BU_C', 'BU_D')
    AND m.dt_periodo >= '2024-01-01'
    AND DATE_TRUNC('month', m.dt_periodo) NOT IN (
        SELECT DISTINCT DATE_TRUNC('month', dt_periodo)
        FROM [schema_auditoria].fato_cdd_demanda_mensal
    )
    AND NOT (
      (d.dc_grupo_distribuidor ILIKE '%DIST_A%' AND pdv.dc_pdv LIKE '%REDE_X%')
      OR (d.dc_grupo_distribuidor ILIKE '%DIST_A%' AND pdv.dc_pdv LIKE '%REDE_Y%')
      OR (d.dc_grupo_distribuidor ILIKE '%DIST_A%' AND pdv.dc_pdv LIKE '%REDE_Z%')
      OR (d.dc_grupo_distribuidor ILIKE '%DIST_B%' AND pdv.dc_pdv LIKE '%REDE_W%')
    )
)

-- ============================================================================
-- SELECT FINAL: Enriquece base bruta com classificações e chaves relacionais
-- ============================================================================
SELECT
  b.tabela_origem,
  b.canal_origem_info,
  DATE_TRUNC('month', b.dt_periodo)   AS ano_mes,
  b.dt_periodo,
  b.cd_apresentacao,

  -- Código SAP ajustado: usa mapeamento inline quando disponível
  COALESCE(sm.cd_sap_ajustado, b.cd_sap) AS cd_sap_ajustado,

  b.cd_ean,
  b.nu_cnpj_pdv,
  b.cd_distribuidor,

  -- Classificação de canal do PDV (hierarquia: associação > subcanal > default)
  CASE
    WHEN b.tp_assoc_franquia = 'ABRAFARMA'                                       THEN 'ABRAFARMA'
    WHEN b.dc_subcanal = 'DIGITAL'                                               THEN 'ABRAFARMA'
    WHEN b.dc_subcanal IN ('GRANDES REDES','MEDIAS REDES','PEQUENAS REDES','OUTROS CANAIS')
         AND b.tp_assoc_franquia != 'ABRAFARMA'                                  THEN 'OUTRAS REDES'
    WHEN b.dc_subcanal IN ('INDEPENDENTES','NCLASS')                             THEN 'INDEPENDENTES'
    ELSE 'ASSOC./FRANQUIA'
  END AS canal_pdv,

  -- Código numérico da BU (usado em chaves relacionais)
  CASE b.dc_divisao
    WHEN 'BU_A' THEN 21
    WHEN 'BU_B' THEN 4
    WHEN 'BU_C' THEN 18
    WHEN 'BU_D' THEN 15
  END AS cd_divisao_num,

  b.qt_unidade,
  b.vl_reais_preco_fabrica,
  b.vl_reais_preco_com_desconto,
  b.vl_reais_preco_consumidor,

  -- Chave relacional cliente (para join com sell-in)
  CONCAT_WS('_', 'sellout',
    CASE b.dc_divisao
      WHEN 'BU_A' THEN 21
      WHEN 'BU_B' THEN 4
      WHEN 'BU_C' THEN 18
      WHEN 'BU_D' THEN 15
    END,
    b.cd_distribuidor
  ) AS chave_cliente,

  -- Chave relacional SKU (para join com dimensão de produtos)
  CONCAT_WS('_',
    CASE b.dc_divisao
      WHEN 'BU_A' THEN 21
      WHEN 'BU_B' THEN 4
      WHEN 'BU_C' THEN 18
      WHEN 'BU_D' THEN 15
    END,
    COALESCE(sm.cd_sap_ajustado, b.cd_sap)
  ) AS sku_id

FROM base_bruta b
LEFT JOIN sap_map sm ON b.cd_apresentacao = sm.cd_apresentacao
