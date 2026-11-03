select
    data,
    valor as selic_meta
from {{ source('bcb', 'selic_meta') }}