import os
from fmu.sumo.explorer import Explorer
from webviz_4d._providers.wellbore_provider._omnia import extract_omnia_session
from webviz_4d._providers.wellbore_provider._msal import get_token
from webviz_4d._datainput.well import (
    load_smda_metadata,
    load_pdm_info,
)
from webviz_4d._providers.wellbore_provider._provider_impl_file import ProviderImplFile


omnia_env = ".omniaapi"
home = os.path.expanduser("~")
omnia_path = os.path.expanduser(os.path.join(home, omnia_env))
field_name = "JOHAN SVERDRUP"

apis = ["SMDA", "PDM", "SUMO"]

for api in apis:
    session = extract_omnia_session(omnia_path, api)
    provider = ProviderImplFile(omnia_path, api)

    if api == "SMDA":
        drilled_wells_info = load_smda_metadata(provider, field_name)
        print(drilled_wells_info)
    elif api == "PDM":
        pdm_wells_info = load_pdm_info(provider, field_name)
        print(drilled_wells_info)
    elif api == "SUMO":
        sumo_case_name = "25p0p3_histandpred_ff_16022025"
        sumo = Explorer(env="prod", keep_alive="20m")
        cases = sumo.cases.filter(name=sumo_case_name)
        my_case = cases[0]
        print(my_case)
