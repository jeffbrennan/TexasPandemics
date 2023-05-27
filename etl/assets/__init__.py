# define assets to be used in pipeline

# region vitals --------------------------------------------------------------------------------


# region county --------------------------------------------------------------------------------


from etl.assets.vitals.get_arcgis_rest import (
    get_vitals_harris,
    get_vitals_bexar,
    get_vitals_travis,
    get_vitals_randall,
    get_vitals_potter,
    get_vitals_denton,
    get_vitals_nueces
)

from etl.assets.vitals.get_power_bi import (
    get_vitals_tarrant,
    get_vitals_el_paso
)

from etl.assets.vitals.get_tableau_data import (
    get_vitals_galveston
)

from etl.assets.vitals.combine_vitals import get_vitals_combined

# endregion

# region state --------------------------------------------------------------------------------
from etl.assets.vitals.get_covid_dshs import (
    new_texas_vitals
)

from etl.assets.vitals.combine_state_vitals import (
    combine_state_vitals
)

# endregion
# endregion


# region wastewater --------------------------------------------------------------------------------
from etl.assets.wastewater.get_houston_wastewater import (
    get_houston_wastewater_plant,
    get_houston_wastewater_zip
)




# endregion
