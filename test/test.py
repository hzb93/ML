from typing import Union

def add(a:Union[int, float], b:int) -> Union[int, float]:
    return a+b


add(1.3,2)

select g.phone,g.clue_record_id,g.clue_created_date,g.is_xf,g.is_valid_clue,g.dealer_code,g.clue_source_name,g.source_type_name,g.series,g.intent_level
from (select t.*,row_number() over(partition by t.phone,t.dealer_code,t.clue_source_name,t.source_type_name,substr(clue_created_date,1,10) order by t.is_xf desc,t.clue_created_date) as rk
from clue_table t) g
where g.rk=1 and phone is not null;
