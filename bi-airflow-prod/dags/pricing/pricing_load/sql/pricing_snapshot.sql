delete from pricing.all_pricing_grover_snapshots
where snapshot_date = current_date;

insert into pricing.all_pricing_grover_snapshots
select *,current_date from pricing.all_pricing_grover;

delete from pricing.prisync_de_snapshots
where snapshot_date = current_date;

insert into pricing.prisync_de_snapshots
select *,current_date from pricing.prisync_de ;

delete from pricing.prisync_us_snapshots
where snapshot_date = current_date;

insert into pricing.prisync_us_snapshots
select *,current_date from pricing.prisync_us ;
