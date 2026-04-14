-- ============================================================================
-- Query: Dimensão Painel Comercial — Indiretos (PDV+ e Associativismo)
-- Descrição: Constrói a dimensão de PDVs do painel comercial de indiretos,
--            unificando dois painéis distintos (PDV+ e Associativismo) com
--            deduplicação por CNPJ + mês base e detecção de overlap entre painéis.
--
-- Padrões utilizados:
--   - ROW_NUMBER() para deduplicação de CNPJs com múltiplos setores
--   - INTERSECT para detecção de overlap entre painéis
--   - EXISTS + subquery correlacionada para flag de overlap no SELECT final
--   - UNION ALL para unificar painéis com estruturas compatíveis
--   - Painel versionado por dt_mes_base (snapshot mensal)
-- ============================================================================

WITH pdv_raw AS (
  -- ========================================================================
  -- CTE 1a: Painel PDV+ — extrai PDVs ativos com estrutura comercial
  -- Deduplicação: um CNPJ pode aparecer em múltiplos setores;
  -- ROW_NUMBER garante apenas uma linha por CNPJ × mês
  -- ========================================================================
  SELECT DISTINCT
    p.dt_mes_base,
    TRY_CAST(p.setor   AS BIGINT) AS cd_setor,
    TRY_CAST(p.nr_cnpj AS BIGINT) AS nu_cnpj_pdv,
    p.bandeira,

    -- Normaliza flag de OL gravada a partir do tipo_painel
    CASE
      WHEN p.tipo_painel LIKE 'N%' THEN 'Não'
      WHEN p.tipo_painel LIKE 'S%' THEN 'Sim'
      ELSE 'Não'
    END AS ol_gravada,

    p.nm_painel AS painel,
    pdv.sg_uf,
    c.cluster,
    c.carteira,

    -- Chave única: painel + CNPJ + mês (usada para joins e deduplicação)
    CONCAT_WS('_',
      p.nm_painel,
      TRY_CAST(p.nr_cnpj AS BIGINT),
      DATE_FORMAT(p.dt_mes_base, 'yyyyMM')
    ) AS chave,

    -- Chaves de estrutura comercial hierárquica (regional > distrital > representante)
    CONCAT_WS(' - ', e.setor_ger_regional,   e.dc_gerente_regional)   AS chave_setor_regional,
    CONCAT_WS(' - ', e.setor_ger_distrital,  e.dc_gerente_distrital)  AS chave_setor_distrital,
    CONCAT_WS(' - ', e.setor,                e.representante)          AS chave_setor_representante,

    -- Chave de cliente SAP para join com sell-in (nullable: CNPJ sem cliente SAP = NULL)
    CASE
      WHEN TRY_CAST(fat.cd_cliente AS BIGINT) IS NULL THEN NULL
      ELSE CONCAT_WS('_', 'sellin_bu', TRY_CAST(fat.cd_cliente AS BIGINT))
    END AS chave_bu_cliente,

    -- Deduplicação: mantém o setor de menor código quando CNPJ aparece em múltiplos
    ROW_NUMBER() OVER (
      PARTITION BY TRY_CAST(p.nr_cnpj AS BIGINT), p.dt_mes_base
      ORDER BY e.setor
    ) AS rn

  FROM [schema_painel].dm_painel_pdv p                              -- painel PDV+ (fonte primária)
    LEFT JOIN [schema_estrutura].dim_estrutura_indiretos e           -- estrutura comercial (regional/distrital/rep)
      ON e.setor = TRY_CAST(p.setor AS BIGINT)
      AND e.time = 'PDV+'
    LEFT JOIN [schema_auditoria].dm_pdv_mensal pdv                   -- dados cadastrais do PDV (UF, etc.)
      ON pdv.nu_cnpj_pdv = TRY_CAST(p.nr_cnpj AS BIGINT)
    LEFT JOIN [schema_estrutura].dim_painel_cluster c                -- cluster estratégico do PDV
      ON c.nu_cnpj_pdv = TRY_CAST(p.nr_cnpj AS BIGINT)
    LEFT JOIN [schema_faturamento].fato_sap_ov_faturada fat          -- join com SAP para obter cd_cliente
      ON TRY_CAST(fat.nu_cnpj_cliente AS BIGINT) = TRY_CAST(p.nr_cnpj AS BIGINT)

  WHERE p.nm_painel = 'PDV+'
    AND TRY_CAST(p.nr_cnpj AS BIGINT) IS NOT NULL
    AND p.dt_mes_base >= '2025-01-01'
),

pdv AS (
  -- Remove coluna auxiliar rn, mantém apenas a linha deduplificada
  SELECT * EXCEPT(rn) FROM pdv_raw WHERE rn = 1
),

assoc_raw AS (
  -- ========================================================================
  -- CTE 1b: Painel Associativismo — extrai PDVs de redes associativas
  -- Estrutura similar ao PDV+, mas com hierarquia distrital (sem representante)
  -- ========================================================================
  SELECT DISTINCT
    p.dt_mes_base,
    TRY_CAST(p.setor   AS BIGINT) AS cd_setor,
    TRY_CAST(p.nr_cnpj AS BIGINT) AS nu_cnpj_pdv,
    p.bandeira,
    COALESCE(p.tipo_painel, 'Não')  AS ol_gravada,
    'ASSOCIATIVISMO'                AS painel,
    pdv.sg_uf,
    'VISITADO'                      AS cluster,
    NULL                            AS carteira,

    CONCAT_WS('_',
      'ASSOCIATIVISMO',
      TRY_CAST(p.nr_cnpj AS BIGINT),
      DATE_FORMAT(p.dt_mes_base, 'yyyyMM')
    ) AS chave,

    CONCAT_WS(' - ', e.setor_ger_regional,  e.dc_gerente_regional)  AS chave_setor_regional,
    CONCAT_WS(' - ', e.setor_ger_distrital, e.dc_gerente_distrital) AS chave_setor_distrital,
    NULL AS chave_setor_representante,   -- Associativismo não tem nível de representante
    NULL AS chave_bu_cliente,

    ROW_NUMBER() OVER (
      PARTITION BY TRY_CAST(p.nr_cnpj AS BIGINT), p.dt_mes_base
      ORDER BY e.setor_ger_distrital
    ) AS rn

  FROM [schema_painel].dm_painel_associativismo p                    -- painel de redes associativas
    LEFT JOIN [schema_estrutura].dim_estrutura_indiretos e
      ON e.setor_ger_distrital = TRY_CAST(p.setor AS BIGINT)
      AND e.time = 'ASSOCIATIVISMO'
    LEFT JOIN [schema_auditoria].dm_pdv_mensal pdv
      ON pdv.nu_cnpj_pdv = TRY_CAST(p.nr_cnpj AS BIGINT)

  WHERE p.nm_painel ILIKE 'ASSOC%'
    AND TRY_CAST(p.nr_cnpj AS BIGINT) IS NOT NULL
    AND p.dt_mes_base >= '2025-01-01'
),

assoc AS (
  SELECT * EXCEPT(rn) FROM assoc_raw WHERE rn = 1
),

cnpjs_overlap AS (
  -- ========================================================================
  -- CTE 2: Detecta CNPJs presentes em AMBOS os painéis no mesmo mês
  -- INTERSECT retorna apenas os pares (cnpj, mês) que existem nas duas CTEs
  -- Usado para sinalizar PDVs com dupla cobertura comercial
  -- ========================================================================
  SELECT DISTINCT nu_cnpj_pdv, dt_mes_base FROM pdv
  INTERSECT
  SELECT DISTINCT nu_cnpj_pdv, dt_mes_base FROM assoc
)

-- ============================================================================
-- SELECT FINAL: Une PDV+ e Associativismo com flag de overlap
-- EXISTS correlacionado verifica se o CNPJ+mês aparece na CTE de overlap
-- ============================================================================
SELECT
  p.*,
  CASE
    WHEN EXISTS (
      SELECT 1 FROM cnpjs_overlap o
      WHERE o.nu_cnpj_pdv = p.nu_cnpj_pdv
        AND o.dt_mes_base  = p.dt_mes_base
    ) THEN 'Sim' ELSE 'Não'
  END AS overlap,
  CONCAT(CEIL(MONTH(p.dt_mes_base) / 3.0), 'ºTRI', YEAR(p.dt_mes_base)) AS trimestre

FROM pdv p

UNION ALL

SELECT
  a.*,
  CASE
    WHEN EXISTS (
      SELECT 1 FROM cnpjs_overlap o
      WHERE o.nu_cnpj_pdv = a.nu_cnpj_pdv
        AND o.dt_mes_base  = a.dt_mes_base
    ) THEN 'Sim' ELSE 'Não'
  END AS overlap,
  CONCAT(CEIL(MONTH(a.dt_mes_base) / 3.0), 'ºTRI', YEAR(a.dt_mes_base)) AS trimestre

FROM assoc a
