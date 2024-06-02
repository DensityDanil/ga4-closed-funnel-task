select STRPOS('view_item,select_item','view_item')
union all 
select STRPOS('view_item,select_item','select_item')
union all 
select STRPOS('view_item,select_item','select_item')- STRPOS('view_item,select_item','view_item')
union all 
select STRPOS('view_item,select_item','view_item')- STRPOS('view_item,select_item','select_item')