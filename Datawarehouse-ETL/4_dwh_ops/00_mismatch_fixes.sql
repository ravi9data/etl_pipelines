drop table if exists dm_operations.mismatch_fixes;
create table dm_operations.mismatch_fixes as
with find_ups_fixes as (
    select report_date,
        serial_number,
        asset_id,
        case
            when report_date = min(
                case when asset_id is not null then report_date end
            ) over (partition by serial_number) then true
            else false
        end is_first_entry_sf,
        case
            when is_first_entry_sf then min(
                case when asset_id is null then report_date end
            ) over (partition by serial_number)
        end entry_date,
        case
            when is_first_entry_sf then datediff ('day', entry_date, report_date) - datediff ('week', entry_date, report_date) * 2 --excluding weekends
        end days_before_booking
    from dm_operations.ups_eu_reconciliation
)
select 'UPS Roermond' as warehouse,
    f.report_date as reporting_date,
    f.serial_number,
    f.asset_id,
    f.days_before_booking,
    a.initial_price
from find_ups_fixes f
    left join master.asset a on f.asset_id = a.asset_id
where f.days_before_booking > 3;
/*
union
select 'Synerlogis' as warehouse,
    reporting_date,
    seriennummer as serial_number,
    asset_id,
    days_before_booking,
    initial_price
from dwh.wemalo_sf_reconciliation
where days_before_booking > 3
*/

GRANT SELECT ON dm_operations.mismatch_fixes TO tableau;
