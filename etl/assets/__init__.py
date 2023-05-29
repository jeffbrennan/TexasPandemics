# define assets to be used in pipeline
# region vitals --------------------------------------------------------------------------------
# region county --------------------------------------------------------------------------------

from etl.assets.origin.vitals.get_arcgis_rest import (
    get_vitals_harris,
    get_vitals_bexar,
    get_vitals_travis,
    get_vitals_randall,
    get_vitals_potter,
    get_vitals_denton,
    get_vitals_nueces
)

from etl.assets.origin.vitals.get_power_bi import (
    get_vitals_tarrant,
    get_vitals_el_paso
)

from etl.assets.origin.vitals.get_tableau_data import (
    get_vitals_galveston
)

from etl.assets.intermediate.vitals.combine_vitals import vitals_combined

from etl.assets.tableau.county_vitals import county_vitals

# endregion

# region state --------------------------------------------------------------------------------
from etl.assets.origin.vitals.get_covid_dshs import (
    new_texas_vitals
)

from etl.assets.intermediate.vitals.combine_state_vitals import (
    combine_state_vitals
)

# endregion
# endregion


# region wastewater --------------------------------------------------------------------------------
from etl.assets.origin.wastewater.get_houston_wastewater import (
    get_houston_wastewater_plant,
    get_houston_wastewater_zip
)

from etl.assets.origin.wastewater.get_cdc_wastewater import (
    get_cdc_wastewater
)

# endregion
