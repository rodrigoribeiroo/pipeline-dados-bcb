with dolar as (
    select * from {{ ref('stg_cotacao_dolar') }}
),

selic as (
    select * from {{ ref('stg_selic_meta') }}
)

select
    d.data,
    d.cotacao_dolar,
    round(
        avg(d.cotacao_dolar) over (
            order by d.data
            rows between 29 preceding and current row
        ), 4
    ) as media_movel_30d,
    round(
        (d.cotacao_dolar / lag(d.cotacao_dolar) over (order by d.data) - 1) * 100, 4
    ) as variacao_diaria_pct,
    s.selic_meta
from dolar d
left join selic s on s.data = d.data
order by d.data