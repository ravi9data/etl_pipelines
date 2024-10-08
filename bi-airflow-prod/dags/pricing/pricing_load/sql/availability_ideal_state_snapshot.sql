insert	into pricing.availability_ideal_state_snapshot(product_sku,
	de,
	"at",
	nl,
	es,
	us,
	"b2b de",
	"b2b at",
	"b2b nl",
	"b2b es",
	mediamarkt,
	mediamarkt_online,
	saturn,
	saturn_online,
	betterworx,
	boltshop_nl,
	comspot_offline,
	comspot_online,
	conrad,
	conrad_online,
	euronics,
	expert_offline,
	gravis,
	irobot,
	mobilcom_debitel,
	otto,
	quelle,
	samsung_online,
	shifter,
	universal,
	weltbild_online,
	asus_mobiles,
	snapshot_date)
select
	product_sku,
	de,
	"at",
	nl,
	es,
	us,
	"b2b de",
	"b2b at",
	"b2b nl",
	"b2b es",
	mediamarkt,
	mediamarkt_online,
	saturn,
	saturn_online,
	betterworx,
	boltshop_nl,
	comspot_offline,
	comspot_online,
	conrad,
	conrad_online,
	euronics,
	expert_offline,
	gravis,
	irobot,
	mobilcom_debitel,
	otto,
	quelle,
	samsung_online,
	shifter,
	universal,
	weltbild_online,
	asus_mobiles,
	current_date as snapshot_date
from
	pricing.availability_ideal_state
