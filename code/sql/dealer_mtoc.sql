insert overwrite table dealer_mtoc_all
select a.*,b.*
from dealer_state_list a
left join (select *from vehicle_base_info where is_tingchan_series=0) b