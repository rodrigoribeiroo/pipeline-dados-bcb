select
    data,
    valor as cotacao_dolar
from {{ source('bcb', 'cotacao_dolar') }}