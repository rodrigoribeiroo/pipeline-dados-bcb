select *
from {{ ref('stg_cotacao_dolar') }}
where cotacao_dolar <= 0