{{ config(severity='warn') }}

select
    data,
    cotacao_dolar,
    variacao_diaria_pct
from {{ ref('mart_dolar_selic') }}
where abs(variacao_diaria_pct) > 5